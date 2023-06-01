pip uninstall kfp -y
pip uninstall kfp-executor -y
KFP_PKG=EXECUTOR pip install -e sdk/python

python -c 'import kfp.executor as e; print(e.x)'