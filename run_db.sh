cd data
rm run.sh run.bat *.zip
for i in $(ls *.sql); do
    echo isql -i $i 192.168.1.148:\"/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base de datos 2021/Migracion de BD/ROCKWELL_MIG.FDB\" -user SYSDBA -pass masterkey >> run.sh
done;
chmod u+x run.sh
./run.sh
cd ..
echo "Done."