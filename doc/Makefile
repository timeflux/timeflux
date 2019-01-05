# Makefile for Sphinx documentation

# General Sphinx variables
SPHINXOPTS    =
SPHINXBUILD   = python -msphinx
SPHINXPROJ    = Timeflux
SOURCEDIR     = .
BUILDDIR      = build

# Publishing variables
FTP           = ftp://user:password@host
SRC           = build/html
DST           = doc/latest

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Publish via FTP
publish:
	@lftp $(FTP) -e "mirror -e --ignore-time -R $(SRC) $(DST); quit"

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@rm -rf api && rm -rf build
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
