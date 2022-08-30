#!/bin/sh

sh dev/install_git_hooks.sh

if [ ! -d python_env ]; then

  virtualenv python_env
  source python_env/bin/activate
  pip install -r requirements.txt

fi




