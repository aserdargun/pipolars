"""
PI Data Extractor - Extract Analyses and PI Points from PI System

This script extracts:
1. AF Analyses from PI AF Server with comprehensive attributes
2. PI Points from PI Data Archive with comprehensive attributes

Uses Windows authentication to connect to the servers.
"""

import json
import sys
import io
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Fix encoding for Windows console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add pipolars to path
sys.path.insert(0, str(Path(__file__).parent / "pipolars" / "src"))

from pipolars.connection.af_database import AFDatabaseConnection
from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.config import AFServerConfig

# Global verbose flag for detailed logging
VERBOSE_LOGGING = True


def log_verbose(message: str) -> None:
    """Print message if verbose logging is enabled."""
    if VERBOSE_LOGGING:
        print(message)


def serialize_datetime(obj: Any) -> Any:
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def convert_net_datetime(net_datetime: Any) -> datetime | None:
    """Convert a .NET DateTime to Python datetime."""
    try:
        return datetime(
            net_datetime.Year,
            net_datetime.Month,
            net_datetime.Day,
            net_datetime.Hour,
            net_datetime.Minute,
            net_datetime.Second,
            net_datetime.Millisecond * 1000,
        )
    except Exception:
        return None


def safe_str(value: Any) -> str | None:
    """Safely convert a value to string."""
    if value is None:
        return None
    try:
        return str(value)
    except Exception:
        return None


def extract_analyses_from_all_databases(af_server: str) -> list[dict[str, Any]]:
    """
    Extract all AF Analyses from ALL databases on the PI AF Server.

    Args:
        af_server: AF Server name (e.g., "GENCOPI")

    Returns:
        List of analysis dictionaries with all attributes and properties.
    """
    sdk = get_sdk_manager()
    sdk.initialize()

    all_analyses = []
    extraction_time = datetime.now(timezone.utc).isoformat()

    # Get PISystems collection to enumerate databases
    PISystems = sdk.get_type("OSIsoft.AF", "PISystems")
    systems = PISystems()
    pi_system = systems[af_server]

    if pi_system is None:
        raise ValueError(f"PI System not found: {af_server}")

    try:
        pi_system.Connect()
        print(f"Connected to AF Server: {pi_system.Name}")

        # Get all databases
        databases = pi_system.Databases
        print(f"Found {databases.Count} databases")

        for db in databases:
            db_name = str(db.Name)
            print(f"\n--- Processing database: {db_name} ---")

            try:
                # Get analyses from this database
                analyses_collection = db.Analyses
                count = analyses_collection.Count
                print(f"  Found {count} analyses in database {db_name}")

                for analysis in analyses_collection:
                    try:
                        analysis_info = extract_analysis_info_raw(analysis, sdk, extraction_time, db_name)
                        all_analyses.append(analysis_info)
                    except Exception as e:
                        print(f"    Error extracting analysis: {e}")

            except Exception as e:
                print(f"  Error accessing database {db_name}: {e}")

        print(f"\nTotal analyses extracted: {len(all_analyses)}")
        return all_analyses

    finally:
        # Always disconnect from the server
        try:
            pi_system.Disconnect()
            print("Disconnected from AF Server")
        except Exception as e:
            print(f"Warning: Error disconnecting from AF Server: {e}")


def extract_analyses(af_server: str, database: str | None = None) -> list[dict[str, Any]]:
    """
    Extract all AF Analyses from the PI AF Server with comprehensive attributes.

    Args:
        af_server: AF Server name (e.g., "GENCOPI")
        database: Optional database name. If None, extracts from ALL databases.

    Returns:
        List of analysis dictionaries with all attributes and properties.
    """
    # If no specific database is specified, extract from all databases
    if database is None:
        return extract_analyses_from_all_databases(af_server)

    config = AFServerConfig(host=af_server, database=database)
    sdk = get_sdk_manager()

    analyses_list = []
    extraction_time = datetime.now(timezone.utc).isoformat()

    with AFDatabaseConnection(config) as conn:
        print(f"Connected to AF Server: {conn.pi_system.Name}")
        print(f"Database: {conn.database.Name}")

        # Initialize the SDK
        sdk.initialize()

        # Access the analyses collection from the database
        af_database = conn.database

        # Get all analyses from the database
        try:
            analyses_collection = af_database.Analyses
            print(f"Found {analyses_collection.Count} analyses in database")

            for analysis in analyses_collection:
                try:
                    analysis_info = extract_analysis_info(analysis, sdk, extraction_time, conn)
                    analyses_list.append(analysis_info)
                    print(f"  Extracted analysis: {analysis_info['Name']}")
                except Exception as e:
                    print(f"  Error extracting analysis: {e}")
                    traceback.print_exc()

        except AttributeError:
            # If Analyses collection not directly available, search for them
            print("Searching for analyses through elements...")
            analyses_list = search_analyses_in_elements(conn, sdk, extraction_time)

    return analyses_list


def extract_analysis_info_raw(analysis: Any, sdk: Any, extraction_time: str, database_name: str) -> dict[str, Any]:
    """
    Extract comprehensive information from an AF Analysis object (raw SDK access).

    Args:
        analysis: AFAnalysis object
        sdk: SDK manager instance
        extraction_time: ISO timestamp of extraction
        database_name: Name of the database

    Returns:
        Dictionary with all analysis attributes and properties in the specified format.
    """
    info: dict[str, Any] = {}

    # Basic identification
    info["Name"] = safe_str(analysis.Name)
    info["Id"] = safe_str(analysis.ID) if hasattr(analysis, "ID") else None
    info["Description"] = safe_str(analysis.Description) if analysis.Description else None
    info["DatabaseName"] = database_name

    log_verbose(f"    [Extracting] {info['Name']}")

    # Analysis Rule Plugin Name
    try:
        if hasattr(analysis, "AnalysisRulePlugIn") and analysis.AnalysisRulePlugIn:
            info["AnalysisRulePlugInName"] = safe_str(analysis.AnalysisRulePlugIn.Name)
            log_verbose(f"      -> AnalysisType: {info['AnalysisRulePlugInName']}")
        else:
            info["AnalysisRulePlugInName"] = None
    except Exception:
        info["AnalysisRulePlugInName"] = None

    # Analysis Type (derived from plugin name)
    info["AnalysisType"] = info.get("AnalysisRulePlugInName")

    # Element Path (Target element)
    try:
        if hasattr(analysis, "Target") and analysis.Target:
            info["ElementPath"] = safe_str(analysis.Target.GetPath())
        else:
            info["ElementPath"] = None
    except Exception:
        info["ElementPath"] = None

    # Get Plant Name and Category from element path
    element_path = info.get("ElementPath") or ""
    info["PlantName"] = extract_plant_name(element_path)
    info["PlantCategory"] = determine_plant_category(element_path)

    # Template Name
    try:
        if hasattr(analysis, "Template") and analysis.Template:
            info["TemplateName"] = safe_str(analysis.Template.Name)
        else:
            info["TemplateName"] = None
    except Exception:
        info["TemplateName"] = None

    # Event Frame Template Name (if applicable)
    info["EventFrameTemplateName"] = None
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            ar = analysis.AnalysisRule
            if hasattr(ar, "ConfigString"):
                config_str = safe_str(ar.ConfigString)
                if config_str and "EFTNAME=" in config_str:
                    parts = config_str.split("EFTNAME=")
                    if len(parts) > 1:
                        eft_name = parts[1].split(";")[0]
                        info["EventFrameTemplateName"] = eft_name
    except Exception:
        pass

    # Config Expression
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            info["ConfigExpression"] = safe_str(analysis.AnalysisRule.ConfigString) if hasattr(analysis.AnalysisRule, "ConfigString") else None
        else:
            info["ConfigExpression"] = None
    except Exception:
        info["ConfigExpression"] = None

    # Status and Enabled - AFAnalysis uses Status property with AFStatus enum
    try:
        if hasattr(analysis, "Status"):
            status = analysis.Status
            # AFStatus enum: Enabled = 0, Disabled = 1
            status_str = safe_str(status)
            info["IsEnabled"] = status_str == "Enabled" if status_str else None
            log_verbose(f"      -> Status: {status_str}, IsEnabled: {info['IsEnabled']}")
        else:
            info["IsEnabled"] = None
            log_verbose(f"      -> Status property not found")
    except Exception as e:
        info["IsEnabled"] = None
        log_verbose(f"      -> Error getting Status: {e}")

    # Schedule Type / Time Rule
    try:
        if hasattr(analysis, "TimeRule") and analysis.TimeRule:
            tr = analysis.TimeRule
            if hasattr(tr, "TimeRulePlugIn") and tr.TimeRulePlugIn:
                info["ScheduleType"] = safe_str(tr.TimeRulePlugIn.Name)
            else:
                info["ScheduleType"] = safe_str(tr.Name) if hasattr(tr, "Name") else None
            log_verbose(f"      -> ScheduleType: {info['ScheduleType']}")
        else:
            info["ScheduleType"] = None
    except Exception:
        info["ScheduleType"] = None

    # Start Trigger / End Trigger / TrueFor / Severity (for event frame analyses)
    info["StartTrigger"] = None
    info["EndTrigger"] = None
    info["TrueFor"] = None
    info["TrueForEnding"] = None
    info["Severity"] = None
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            ar = analysis.AnalysisRule
            log_verbose(f"      -> AnalysisRule type: {type(ar).__name__}")

            # List available attributes on the rule for debugging
            ar_attrs = [attr for attr in dir(ar) if not attr.startswith('_')]
            log_verbose(f"      -> AnalysisRule attrs (first 15): {ar_attrs[:15]}")

            # Try to get Expression (StartTrigger)
            if hasattr(ar, "Expression"):
                info["StartTrigger"] = safe_str(ar.Expression)

            # For EventFrame type analyses, the AnalysisRule might be AFEventFrameGenerationRule
            # Try accessing properties directly on the rule
            if hasattr(ar, "TrueFor"):
                true_for_raw = ar.TrueFor
                log_verbose(f"      -> TrueFor raw: {true_for_raw}, type: {type(true_for_raw)}")
                if true_for_raw:
                    # TrueFor is typically a TimeSpan
                    if hasattr(true_for_raw, "TotalSeconds"):
                        total_seconds = true_for_raw.TotalSeconds
                        log_verbose(f"      -> TrueFor TotalSeconds: {total_seconds}")
                        if total_seconds > 0:
                            info["TrueFor"] = safe_str(true_for_raw)
                    else:
                        info["TrueFor"] = safe_str(true_for_raw)
            else:
                log_verbose(f"      -> TrueFor attribute not found on AnalysisRule")

            if hasattr(ar, "TrueForEnding") and ar.TrueForEnding:
                true_for_ending = ar.TrueForEnding
                if hasattr(true_for_ending, "TotalSeconds"):
                    total_seconds = true_for_ending.TotalSeconds
                    if total_seconds > 0:
                        info["TrueForEnding"] = safe_str(true_for_ending)
                else:
                    info["TrueForEnding"] = safe_str(true_for_ending)

            # Try to get EndTrigger
            if hasattr(ar, "EndExpression"):
                info["EndTrigger"] = safe_str(ar.EndExpression)

            # Try to get Severity from the rule
            if hasattr(ar, "Severity"):
                severity_raw = ar.Severity
                log_verbose(f"      -> Severity raw: {severity_raw}, type: {type(severity_raw)}")
                if severity_raw:
                    info["Severity"] = safe_str(severity_raw)
            else:
                log_verbose(f"      -> Severity attribute not found on AnalysisRule")

            # Also try to get from nested PlugIn config if available
            if hasattr(ar, "PlugIn") and ar.PlugIn:
                plugin = ar.PlugIn
                plugin_name = safe_str(plugin.Name) if hasattr(plugin, "Name") else None
                log_verbose(f"      -> PlugIn: {plugin_name}")
                if plugin_name and "EventFrame" in plugin_name:
                    # This is an event frame generation rule
                    if hasattr(ar, "GetConfiguration"):
                        try:
                            config = ar.GetConfiguration()
                            log_verbose(f"      -> Got configuration: {type(config)}")
                            if config and hasattr(config, "TrueFor"):
                                info["TrueFor"] = safe_str(config.TrueFor)
                                log_verbose(f"      -> TrueFor from config: {info['TrueFor']}")
                            if config and hasattr(config, "Severity"):
                                info["Severity"] = safe_str(config.Severity)
                                log_verbose(f"      -> Severity from config: {info['Severity']}")
                        except Exception as cfg_e:
                            log_verbose(f"      -> Error getting config: {cfg_e}")
        else:
            log_verbose(f"      -> No AnalysisRule found")
    except Exception as e:
        log_verbose(f"      -> Error extracting rule properties: {e}")

    log_verbose(f"      -> Final: IsEnabled={info['IsEnabled']}, TrueFor={info['TrueFor']}, Severity={info['Severity']}")

    # Input/Output counts
    info["InputCount"] = 0
    info["OutputCount"] = 0
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            ar = analysis.AnalysisRule
            if hasattr(ar, "VariableMapping"):
                info["InputCount"] = len(list(ar.VariableMapping)) if ar.VariableMapping else 0
            if hasattr(ar, "Outputs"):
                info["OutputCount"] = len(list(ar.Outputs)) if ar.Outputs else 0
    except Exception:
        pass

    # Rollup information
    info["RollupType"] = None
    info["RollupSource"] = None
    info["RollupInputAttribute"] = None
    info["RollupOutputAttribute"] = None

    # Severity (if not already set from AnalysisRule)
    if info.get("Severity") is None:
        try:
            if hasattr(analysis, "Severity") and analysis.Severity:
                info["Severity"] = safe_str(analysis.Severity)
        except Exception:
            pass

    # Version
    try:
        info["Version"] = int(analysis.Version) if hasattr(analysis, "Version") else 0
    except Exception:
        info["Version"] = 0

    # Checkout information
    info["IsCheckedOut"] = False
    info["CheckedOutBy"] = None
    info["CheckedInDate"] = None
    try:
        if hasattr(analysis, "IsCheckedOut"):
            info["IsCheckedOut"] = bool(analysis.IsCheckedOut)
        if hasattr(analysis, "CheckedOutBy"):
            info["CheckedOutBy"] = safe_str(analysis.CheckedOutBy)
        if hasattr(analysis, "CheckedInDate") and analysis.CheckedInDate:
            info["CheckedInDate"] = convert_net_datetime(analysis.CheckedInDate.LocalTime)
    except Exception:
        pass

    # IsDirty
    try:
        info["IsDirty"] = bool(analysis.IsDirty) if hasattr(analysis, "IsDirty") else False
    except Exception:
        info["IsDirty"] = False

    # Creation and modification info
    info["CreateDate"] = None
    info["CreatedBy"] = None
    info["ModifyDate"] = None
    info["ModifiedBy"] = None
    try:
        if hasattr(analysis, "CreationDate") and analysis.CreationDate:
            info["CreateDate"] = convert_net_datetime(analysis.CreationDate.LocalTime)
        if hasattr(analysis, "CreatedBy"):
            info["CreatedBy"] = safe_str(analysis.CreatedBy)
        if hasattr(analysis, "ModifyDate") and analysis.ModifyDate:
            info["ModifyDate"] = convert_net_datetime(analysis.ModifyDate.LocalTime)
        if hasattr(analysis, "ModifiedBy"):
            info["ModifiedBy"] = safe_str(analysis.ModifiedBy)
    except Exception:
        pass

    # Extraction timestamp
    info["ExtractedAt"] = extraction_time

    return info


def extract_analysis_info(analysis: Any, sdk: Any, extraction_time: str, conn: AFDatabaseConnection) -> dict[str, Any]:
    """
    Extract comprehensive information from an AF Analysis object.

    Args:
        analysis: AFAnalysis object
        sdk: SDK manager instance
        extraction_time: ISO timestamp of extraction
        conn: AF Database connection

    Returns:
        Dictionary with all analysis attributes and properties in the specified format.
    """
    info: dict[str, Any] = {}

    # Basic identification
    info["Name"] = safe_str(analysis.Name)
    info["Id"] = safe_str(analysis.ID) if hasattr(analysis, "ID") else None
    info["Description"] = safe_str(analysis.Description) if analysis.Description else None

    # Analysis Rule Plugin Name
    try:
        if hasattr(analysis, "AnalysisRulePlugIn") and analysis.AnalysisRulePlugIn:
            info["AnalysisRulePlugInName"] = safe_str(analysis.AnalysisRulePlugIn.Name)
        else:
            info["AnalysisRulePlugInName"] = None
    except Exception:
        info["AnalysisRulePlugInName"] = None

    # Analysis Type (derived from plugin name)
    info["AnalysisType"] = info.get("AnalysisRulePlugInName")

    # Element Path (Target element)
    try:
        if hasattr(analysis, "Target") and analysis.Target:
            info["ElementPath"] = safe_str(analysis.Target.GetPath())
        else:
            info["ElementPath"] = None
    except Exception:
        info["ElementPath"] = None

    # Get Plant Name and Category from element path
    element_path = info.get("ElementPath") or ""
    info["PlantName"] = extract_plant_name(element_path)
    info["PlantCategory"] = determine_plant_category(element_path)

    # Template Name
    try:
        if hasattr(analysis, "Template") and analysis.Template:
            info["TemplateName"] = safe_str(analysis.Template.Name)
        else:
            info["TemplateName"] = None
    except Exception:
        info["TemplateName"] = None

    # Event Frame Template Name (if applicable)
    info["EventFrameTemplateName"] = None
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            ar = analysis.AnalysisRule
            # Try to get event frame template from config string
            if hasattr(ar, "ConfigString"):
                config_str = safe_str(ar.ConfigString)
                if config_str and "EFTNAME=" in config_str:
                    # Extract template name from config
                    parts = config_str.split("EFTNAME=")
                    if len(parts) > 1:
                        eft_name = parts[1].split(";")[0]
                        info["EventFrameTemplateName"] = eft_name
    except Exception:
        pass

    # Config Expression
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            info["ConfigExpression"] = safe_str(analysis.AnalysisRule.ConfigString) if hasattr(analysis.AnalysisRule, "ConfigString") else None
        else:
            info["ConfigExpression"] = None
    except Exception:
        info["ConfigExpression"] = None

    # Status and Enabled - AFAnalysis uses Status property with AFStatus enum
    try:
        if hasattr(analysis, "Status"):
            status = analysis.Status
            # AFStatus enum: Enabled = 0, Disabled = 1
            status_str = safe_str(status)
            info["IsEnabled"] = status_str == "Enabled" if status_str else None
            log_verbose(f"      -> Status: {status_str}, IsEnabled: {info['IsEnabled']}")
        else:
            info["IsEnabled"] = None
            log_verbose(f"      -> Status property not found")
    except Exception as e:
        info["IsEnabled"] = None
        log_verbose(f"      -> Error getting Status: {e}")

    # Schedule Type / Time Rule
    try:
        if hasattr(analysis, "TimeRule") and analysis.TimeRule:
            tr = analysis.TimeRule
            if hasattr(tr, "TimeRulePlugIn") and tr.TimeRulePlugIn:
                info["ScheduleType"] = safe_str(tr.TimeRulePlugIn.Name)
            else:
                info["ScheduleType"] = safe_str(tr.Name) if hasattr(tr, "Name") else None
            log_verbose(f"      -> ScheduleType: {info['ScheduleType']}")
        else:
            info["ScheduleType"] = None
    except Exception:
        info["ScheduleType"] = None

    # Start Trigger / End Trigger / TrueFor / Severity (for event frame analyses)
    info["StartTrigger"] = None
    info["EndTrigger"] = None
    info["TrueFor"] = None
    info["TrueForEnding"] = None
    info["Severity"] = None
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            ar = analysis.AnalysisRule
            log_verbose(f"      -> AnalysisRule type: {type(ar).__name__}")

            # List available attributes on the rule for debugging
            ar_attrs = [attr for attr in dir(ar) if not attr.startswith('_')]
            log_verbose(f"      -> AnalysisRule attrs (first 15): {ar_attrs[:15]}")

            # Try to get Expression (StartTrigger)
            if hasattr(ar, "Expression"):
                info["StartTrigger"] = safe_str(ar.Expression)

            # For EventFrame type analyses, the AnalysisRule might be AFEventFrameGenerationRule
            # Try accessing properties directly on the rule
            if hasattr(ar, "TrueFor"):
                true_for_raw = ar.TrueFor
                log_verbose(f"      -> TrueFor raw: {true_for_raw}, type: {type(true_for_raw)}")
                if true_for_raw:
                    # TrueFor is typically a TimeSpan
                    if hasattr(true_for_raw, "TotalSeconds"):
                        total_seconds = true_for_raw.TotalSeconds
                        log_verbose(f"      -> TrueFor TotalSeconds: {total_seconds}")
                        if total_seconds > 0:
                            info["TrueFor"] = safe_str(true_for_raw)
                    else:
                        info["TrueFor"] = safe_str(true_for_raw)
            else:
                log_verbose(f"      -> TrueFor attribute not found on AnalysisRule")

            if hasattr(ar, "TrueForEnding") and ar.TrueForEnding:
                true_for_ending = ar.TrueForEnding
                if hasattr(true_for_ending, "TotalSeconds"):
                    total_seconds = true_for_ending.TotalSeconds
                    if total_seconds > 0:
                        info["TrueForEnding"] = safe_str(true_for_ending)
                else:
                    info["TrueForEnding"] = safe_str(true_for_ending)

            # Try to get EndTrigger
            if hasattr(ar, "EndExpression"):
                info["EndTrigger"] = safe_str(ar.EndExpression)

            # Try to get Severity from the rule
            if hasattr(ar, "Severity"):
                severity_raw = ar.Severity
                log_verbose(f"      -> Severity raw: {severity_raw}, type: {type(severity_raw)}")
                if severity_raw:
                    info["Severity"] = safe_str(severity_raw)
            else:
                log_verbose(f"      -> Severity attribute not found on AnalysisRule")

            # Also try to get from nested PlugIn config if available
            if hasattr(ar, "PlugIn") and ar.PlugIn:
                plugin = ar.PlugIn
                plugin_name = safe_str(plugin.Name) if hasattr(plugin, "Name") else None
                log_verbose(f"      -> PlugIn: {plugin_name}")
                if plugin_name and "EventFrame" in plugin_name:
                    # This is an event frame generation rule
                    if hasattr(ar, "GetConfiguration"):
                        try:
                            config = ar.GetConfiguration()
                            log_verbose(f"      -> Got configuration: {type(config)}")
                            if config and hasattr(config, "TrueFor"):
                                info["TrueFor"] = safe_str(config.TrueFor)
                                log_verbose(f"      -> TrueFor from config: {info['TrueFor']}")
                            if config and hasattr(config, "Severity"):
                                info["Severity"] = safe_str(config.Severity)
                                log_verbose(f"      -> Severity from config: {info['Severity']}")
                        except Exception as cfg_e:
                            log_verbose(f"      -> Error getting config: {cfg_e}")
        else:
            log_verbose(f"      -> No AnalysisRule found")
    except Exception as e:
        log_verbose(f"      -> Error extracting rule properties: {e}")

    log_verbose(f"      -> Final: IsEnabled={info['IsEnabled']}, TrueFor={info['TrueFor']}, Severity={info['Severity']}")

    # Input/Output counts
    info["InputCount"] = 0
    info["OutputCount"] = 0
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            ar = analysis.AnalysisRule
            if hasattr(ar, "VariableMapping"):
                info["InputCount"] = len(list(ar.VariableMapping)) if ar.VariableMapping else 0
            if hasattr(ar, "Outputs"):
                info["OutputCount"] = len(list(ar.Outputs)) if ar.Outputs else 0
    except Exception:
        pass

    # Rollup information
    info["RollupType"] = None
    info["RollupSource"] = None
    info["RollupInputAttribute"] = None
    info["RollupOutputAttribute"] = None
    try:
        if hasattr(analysis, "AnalysisRule") and analysis.AnalysisRule:
            ar = analysis.AnalysisRule
            if hasattr(ar, "RollupType"):
                info["RollupType"] = safe_str(ar.RollupType)
            if hasattr(ar, "RollupSource"):
                info["RollupSource"] = safe_str(ar.RollupSource)
    except Exception:
        pass

    # Severity (if not already set from AnalysisRule)
    if info.get("Severity") is None:
        try:
            if hasattr(analysis, "Severity") and analysis.Severity:
                info["Severity"] = safe_str(analysis.Severity)
        except Exception:
            pass

    # Version
    try:
        info["Version"] = int(analysis.Version) if hasattr(analysis, "Version") else 0
    except Exception:
        info["Version"] = 0

    # Checkout information
    info["IsCheckedOut"] = False
    info["CheckedOutBy"] = None
    info["CheckedInDate"] = None
    try:
        if hasattr(analysis, "IsCheckedOut"):
            info["IsCheckedOut"] = bool(analysis.IsCheckedOut)
        if hasattr(analysis, "CheckedOutBy"):
            info["CheckedOutBy"] = safe_str(analysis.CheckedOutBy)
        if hasattr(analysis, "CheckedInDate") and analysis.CheckedInDate:
            info["CheckedInDate"] = convert_net_datetime(analysis.CheckedInDate.LocalTime)
    except Exception:
        pass

    # IsDirty
    try:
        info["IsDirty"] = bool(analysis.IsDirty) if hasattr(analysis, "IsDirty") else False
    except Exception:
        info["IsDirty"] = False

    # Creation and modification info
    info["CreateDate"] = None
    info["CreatedBy"] = None
    info["ModifyDate"] = None
    info["ModifiedBy"] = None
    try:
        if hasattr(analysis, "CreationDate") and analysis.CreationDate:
            info["CreateDate"] = convert_net_datetime(analysis.CreationDate.LocalTime)
        if hasattr(analysis, "CreatedBy"):
            info["CreatedBy"] = safe_str(analysis.CreatedBy)
        if hasattr(analysis, "ModifyDate") and analysis.ModifyDate:
            info["ModifyDate"] = convert_net_datetime(analysis.ModifyDate.LocalTime)
        if hasattr(analysis, "ModifiedBy"):
            info["ModifiedBy"] = safe_str(analysis.ModifiedBy)
    except Exception:
        pass

    # Extraction timestamp
    info["ExtractedAt"] = extraction_time

    return info


def extract_plant_name(element_path: str) -> str | None:
    """Extract plant name from element path."""
    if not element_path:
        return None
    try:
        # Path format: \\SERVER\Database\Category\PlantName
        parts = element_path.strip("\\").split("\\")
        if len(parts) >= 4:
            return parts[3]  # Plant name is usually the 4th part
        elif len(parts) >= 3:
            return parts[2]
        return None
    except Exception:
        return None


def determine_plant_category(element_path: str) -> int | None:
    """Determine plant category from element path."""
    if not element_path:
        return None
    try:
        path_lower = element_path.lower()
        if "hydro" in path_lower:
            return 1  # Hydro
        elif "thermal" in path_lower or "coal" in path_lower:
            return 2  # Thermal
        elif "solar" in path_lower or "ges" in path_lower:
            return 3  # Solar
        elif "wind" in path_lower or "res" in path_lower:
            return 4  # Wind
        elif "gas" in path_lower:
            return 5  # Gas
        else:
            return 0  # Unknown
    except Exception:
        return None


def search_analyses_in_elements(conn: AFDatabaseConnection, sdk: Any, extraction_time: str) -> list[dict[str, Any]]:
    """
    Search for analyses by traversing elements in the database.

    Args:
        conn: AF Database connection
        sdk: SDK manager instance
        extraction_time: ISO timestamp of extraction

    Returns:
        List of analysis dictionaries.
    """
    analyses_list = []

    def traverse_elements(elements: Any, depth: int = 0) -> None:
        """Recursively traverse elements to find analyses."""
        for element in elements:
            # Check for analyses on this element
            try:
                if hasattr(element, "Analyses") and element.Analyses:
                    for analysis in element.Analyses:
                        try:
                            analysis_info = extract_analysis_info(analysis, sdk, extraction_time, conn)
                            analyses_list.append(analysis_info)
                            print(f"  {'  ' * depth}Found analysis: {analysis_info['Name']} on element: {element.Name}")
                        except Exception as e:
                            print(f"  {'  ' * depth}Error extracting analysis: {e}")
            except Exception as e:
                print(f"  {'  ' * depth}Error accessing analyses on {element.Name}: {e}")

            # Recurse into child elements
            try:
                if element.Elements and element.Elements.Count > 0:
                    traverse_elements(element.Elements, depth + 1)
            except Exception:
                pass

    # Start traversal from root elements
    try:
        root_elements = conn.database.Elements
        print(f"Traversing {root_elements.Count} root elements...")
        traverse_elements(root_elements)
    except Exception as e:
        print(f"Error traversing elements: {e}")

    return analyses_list


def main() -> None:
    """Main entry point for the AF Analyses extraction script."""
    # Configuration - modify these values for your environment
    AF_SERVER = "GENCOPI"  # PI AF Server name
    AF_DATABASE = None  # Set to specific database name or None for all databases

    output_dir = Path(__file__).parent

    print("=" * 60)
    print("AF Analyses Extractor")
    print("=" * 60)
    print(f"AF Server: {AF_SERVER}")
    print(f"AF Database: {AF_DATABASE or 'All Databases'}")
    print("=" * 60)

    # Extract Analyses
    print("\nExtracting AF Analyses...")
    print("-" * 40)
    try:
        analyses = extract_analyses(AF_SERVER, AF_DATABASE)
        print(f"\nExtracted {len(analyses)} analyses")

        # Save to JSON
        analyses_file = output_dir / "analyses.json"
        with open(analyses_file, "w", encoding="utf-8") as f:
            json.dump(analyses, f, indent=2, default=serialize_datetime, ensure_ascii=False)
        print(f"Saved analyses to: {analyses_file}")

    except Exception as e:
        print(f"Error extracting analyses: {e}")
        traceback.print_exc()
        analyses = []

    # Summary
    print("\n" + "=" * 60)
    print("Extraction Complete!")
    print("=" * 60)
    print(f"Analyses extracted: {len(analyses)}")
    print("=" * 60)


if __name__ == "__main__":
    import logging

    # Configure logging to file
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("extraction.log", mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        raise
