# Makefile for Sphinx documentation

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Custom targets

# Build HTML documentation
html:
	@$(SPHINXBUILD) -b html "$(SOURCEDIR)" "$(BUILDDIR)/html" $(SPHINXOPTS) $(O)
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

# Build and watch for changes (requires sphinx-autobuild)
livehtml:
	sphinx-autobuild "$(SOURCEDIR)" "$(BUILDDIR)/html" $(SPHINXOPTS) $(O)

# Clean build directory
clean:
	rm -rf $(BUILDDIR)/*

# Build and open in browser (Windows)
open: html
	start $(BUILDDIR)/html/index.html

# Check for broken links
linkcheck:
	@$(SPHINXBUILD) -b linkcheck "$(SOURCEDIR)" "$(BUILDDIR)/linkcheck" $(SPHINXOPTS) $(O)
	@echo "Link check complete. Results are in $(BUILDDIR)/linkcheck."

# Generate API documentation
apidoc:
	sphinx-apidoc -f -o api ../src/pipolars

# Build PDF (requires latexmk)
pdf:
	@$(SPHINXBUILD) -b latex "$(SOURCEDIR)" "$(BUILDDIR)/latex" $(SPHINXOPTS) $(O)
	@cd $(BUILDDIR)/latex && latexmk -pdf -interaction=nonstopmode
	@echo "Build finished. The PDF is in $(BUILDDIR)/latex."
