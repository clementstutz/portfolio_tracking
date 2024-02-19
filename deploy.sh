#!/bin/bash

venv_name=".venv"

if [[ -d "${venv_dir}" ]]; then
    rm -rf ${venv_dir}
fi
python3 -m venv ${venv_name}
. ./${venv_name}/Scripts/activate

pip3 install yfinance
pip3 install matplotlib
pip3 install --editable .[dev]
