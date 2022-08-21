#!/bin/sh

sh dev/install_git_hooks.sh

if [ ! -d pplns_python ]; then

  virtualenv pplns_python
  source pplns_python/bin/activate
  pip install -r requirements.txt

fi




