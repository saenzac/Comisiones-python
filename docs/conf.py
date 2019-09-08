# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

#We need to tell to Sphinx where is our project. In this case the modules are 1 step up path, that is like "cd .."
sys.path.insert(0, os.path.abspath('../'))


# -- Project information -----------------------------------------------------
project = 'comisiones-python'
copyright = '2019, Johnny Saenz'
author = 'Johnny Saenz'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

#This extension generates function/method/attribute summary lists, similar to those output 
#e.g. by Epydoc and other API doc generation tools. This is especially useful when your 
#docstrings are long and detailed, and putting each one of them on a separate page makes them easier to read.
extensions = [ 'sphinx.ext.autosummary'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '**/Scripts']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
# More themes at: https://sphinx-themes.org/
html_theme = 'nature'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


autosummary_generate = True
numpydoc_show_class_members = False