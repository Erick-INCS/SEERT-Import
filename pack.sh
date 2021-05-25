cd data
rm run.sh run.bat
for i in $(ls *.sql); do
    echo isql -i $i 192.168.1.148:\"/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base de datos 2021/Migracion de BD/ROCKWELL_MIG.FDB\" -user SYSDBA -pass masterkey >> run.bat
done;
zip -r data *.sql *.bat
rm *.sql
cd data
scp data.zip Erick@192.168.1.179:Documents/
cd ..