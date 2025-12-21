@ECHO OFF

pushd %~dp0

REM Command file for Sphinx documentation

if "%SPHINXBUILD%" == "" (
	set SPHINXBUILD=sphinx-build
)
set SOURCEDIR=.
set BUILDDIR=_build

%SPHINXBUILD% >NUL 2>NUL
if errorlevel 9009 (
	echo.
	echo.The 'sphinx-build' command was not found. Make sure you have Sphinx
	echo.installed, then set the SPHINXBUILD environment variable to point
	echo.to the full path of the 'sphinx-build' executable. Alternatively you
	echo.may add the Sphinx directory to PATH.
	echo.
	echo.If you don't have Sphinx installed, grab it from
	echo.https://www.sphinx-doc.org/
	exit /b 1
)

if "%1" == "" goto help
if "%1" == "help" goto help
if "%1" == "html" goto html
if "%1" == "clean" goto clean
if "%1" == "livehtml" goto livehtml

%SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
goto end

:help
%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
goto end

:html
%SPHINXBUILD% -b html %SOURCEDIR% %BUILDDIR%\html %SPHINXOPTS% %O%
echo.Build finished. The HTML pages are in %BUILDDIR%\html.
goto end

:clean
rmdir /s /q %BUILDDIR% 2>NUL
echo.Build directory cleaned.
goto end

:livehtml
sphinx-autobuild %SOURCEDIR% %BUILDDIR%\html %SPHINXOPTS% %O%
goto end

:end
popd
