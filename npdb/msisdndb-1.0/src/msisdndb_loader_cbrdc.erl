-module(msisdndb_loader_cbrdc).

-export([init/0, file_type/2]).

init () ->
  {ok, ReFull} = re:compile("^rdc_resync_(\\d{4})_(\\d{2})_(\\d{2})_(\\d{2})_(\\d{2})\\.csv$"),
  ReFull.

file_type(Name, ReFull) ->
  case re:run (Name, ReFull, [{capture, all, list}]) of
    {match, [_,FYY,FMM,FDD,FHH,FMi]} ->
      FYYi = list_to_integer(FYY),
      FMMi = list_to_integer(FMM),
      FDDi = list_to_integer(FDD),
      FHHi = list_to_integer(FHH),
      FMii = list_to_integer(FMi),
      FSSi = 0,
      FT = {{FYYi, FMMi, FDDi}, {FHHi, FMii, FSSi}},
      {full, FT};
    nomatch -> nomatch
  end.
