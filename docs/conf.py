"""Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html

-- Project information -----------------------------------------------------
https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path("..").resolve()))

project = "Jasmin Metrics Client"
copyright = "2024, (C) Science and Technology Facilities Council"
author = "StTysh"
release = "0.0.0dev0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "autoapi.extension",
    "sphinx-pydantic",
    "sphinx_rtd_theme",
    "sphinx_mdinclude",
    "sphinx.ext.napoleon",
]
templates_path = ["_templates"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# Autoapi Configuration
autoapi_dirs = ["../jasmin_metrics_client", "../tests"]
autoapi_keep_files = True
master_doc = "index"
autoapi_options = [
    "members",
    "undoc-members",
    "private-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
    "imported-members",
]
