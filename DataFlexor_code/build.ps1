# Right Click the file and select run in powersheel
#rm malib.zip
#Compress-Archive *.* malib.zip
pip install wheel
python setup.py bdist_wheel
rm build -r -force
rm DataFlexor.egg-info -r -force
mv dist/DataFlexor-1.3.1-py3-none-any.whl ../library/DataFlexor-1.3.1-py3-none-any.whl -force
rm dist -r -force