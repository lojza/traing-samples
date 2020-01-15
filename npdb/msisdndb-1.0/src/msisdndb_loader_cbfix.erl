-module(msisdndb_loader_cbfix).

-export([init/0, file_type/2]).

init () ->
  {ok, ReIncr} = re:compile("^NPDB(\\d{4})(\\d{2})(\\d{2})-(\\d{2})(\\d{2})\\.txt$"),
  {ok, ReFull} = re:compile("^fix_resync_(\\d{4})_(\\d{2})_(\\d{2})_(\\d{2})_(\\d{2})\\.csv$"),
  {ReIncr, ReFull}.

file_type(Name, {ReIncr, ReFull}) ->
  IncrMatch = case re:run (Name, ReIncr, [{capture, all, list}]) of
    {match, [_,IYY,IMM,IDD,IHH,IMi]} ->
      IYYi = list_to_integer(IYY),
      IMMi = list_to_integer(IMM),
      IDDi = list_to_integer(IDD),
      IHHi = list_to_integer(IHH),
      IMii = list_to_integer(IMi),
      ISSi = 0,
      IT = {{IYYi, IMMi, IDDi}, {IHHi, IMii, ISSi}},
      {increment, IT};
    nomatch -> nomatch
  end,

  FullMatch = case re:run (Name, ReFull, [{capture, all, list}]) of
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
  end,

  if
    IncrMatch /= nomatch -> IncrMatch;
    FullMatch /= nomatch -> FullMatch;
    true -> nomatch
  end.
  %full
  %increment
  %skip
