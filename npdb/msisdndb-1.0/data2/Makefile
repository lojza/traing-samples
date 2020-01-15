RDC=rdc_resync_2018_01_01_00_00_00.csv
FIX=fix_resync_2018_01_01_00_00_00.csv
MOB=mob_resync_2018_01_01_00_00_00.csv
EXTRA=extra.txt

all: $(FIX) $(RDC) $(MOB) $(EXTRA)

rdc: $(RDC)
fix: $(FIX)
mob: $(MOB)
extra: $(EXTRA)
rdc: $(RDC)

clean:
	rm -f $(RDC) $(FIX) $(MOB) $(EXTRA)

$(RDC):
	./make_data.escript rdc 200000 >$(RDC)

$(FIX):
	./make_data.escript fix 300000 >$(FIX)

$(MOB):
	./make_data.escript mob 2500000 >$(MOB)

$(EXTRA):
	./make_data.escript extra 1000 >$(EXTRA)
