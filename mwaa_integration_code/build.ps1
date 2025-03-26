# Right Click the file and select run in powersheel
#rm malib.zip
#Compress-Archive *.* malib.zip
pip install wheel
python setup.py bdist_wheel
rm build -r -force
rm DataFlexorMWAA.egg-info -r -force
mv dist/DataFlexorMWAA-1.0.0-py3-none-any.whl ../mwaa_integration_library/DataFlexorMWAA-1.0.0-py3-none-any.whl -force
rm dist -r -force