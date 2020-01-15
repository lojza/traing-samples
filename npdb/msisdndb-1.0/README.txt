MSISDN dabase application
=========================

usage:

   erl -pa ebin

   msisdndb:start().


   // tydle NE
   msisdndb_extra:read_file("./data2/extra.txt").
   msisdndb_npmob:read_file("./data2/mob_resync_2018_01_01_00_00_00.csv").
   msisdndb_rdc:read_file("./data2/rdc_resync_2018_01_01_00_00_00.csv").
   msisdndb_npfix:read_file("./data2/fix_resync_2018_01_01_00_00_00.csv").

   // tydle jo
   msisdndb_extra:read_file("../data2/extra.txt").
   msisdndb_npmob:read_file("../data2/mob_resync_2017_06_13_00_00_14.csv").
   msisdndb_rdc:read_file("../data2/rdc_resync_2017_06_16_00_00.csv").
   msisdndb_npfix:read_file("../data2/fix_resync_2017_06_19_00_00.csv").


   msisdndb:lookup(790000147).
   msisdndb:oper_id(790000147).

   c(fcg).
   fcg:run(msisdndb,lookup,[790000147], 1000000).
