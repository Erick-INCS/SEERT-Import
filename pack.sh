cd data
rm run.bat
for i in $(ls *.sql); do
    echo isql -i $i 192.168.1.148:\"/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base Datos 2020/Rockwell-UPD-31Dic-2020.fdb\" -user SYSDBA -pass masterkey >> run.bat
done;
zip -r data *.sql *.bat
rm *.sql
scp data/data.zip Erick@192.168.1.179:Documents/data
