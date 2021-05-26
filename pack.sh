rm -r data/
mkdir data
cd data
for i in $(ls | sort -n); do
    echo isql -i $i 192.168.1.148:\"/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base de datos 2021/Migracion de BD/ROCKWELL_MIG.FDB\" -user SYSDBA -pass masterkey >> run.bat
done;
zip -r data .
scp data.zip Erick@192.168.1.179:Documents/
cd ..