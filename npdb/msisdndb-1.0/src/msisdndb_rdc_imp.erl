%%
%% RDC import file callbacks
%%
-module(msisdndb_rdc_imp).

-include("msisdndb_logging.hrl").

-export([read_file/1]).
-export([init/1, process_line/3, make_result/1]).
-record(st, {
  split_patt,
  counter,
  values
}).

read_file (FileName) ->
    msisdndb_imp_utils:read_file (FileName, ?MODULE).

init(_Opts) ->
   St = #st{
     counter = 0,
     split_patt = binary:compile_pattern([<<";">>]),
     values = []
   },
   {ok, St}.

% process line
process_line (BinLine, _LineNum, #st{counter = N, split_patt = Patt, values = Values} = St) ->
    if (N rem 100000 == 0) and (N > 0) -> ?LOG_INFO ("RDC import records: ~p", [N]);
       true -> ok
    end,

    Fields = binary:split(BinLine, Patt, [global]),
    M1 = erlang:binary_to_integer(lists:nth(2,Fields)),
    M2 = erlang:binary_to_integer(lists:nth(3,Fields)),
    Id1 =  lists:nth(7,Fields),
    Id2 = lists:nth(8,Fields),

    OpId = case Id1 of
      <<"">> -> erlang:binary_to_integer(Id2);
      _ -> erlang:binary_to_integer(Id1)
    end,

    %msisdndb_rdc:insert_async (M1, M2, OpId),
    Data = {M1, M2, OpId},

    {ok, St#st{counter = N + 1, values = [Data|Values]}}.

make_result(#st{counter = N, values = Values}) ->
    ?LOG_INFO ("RDC import records: ~p", [N]),
    case msisdndb_rdc:insert_many (Values) of
      ok -> {ok, N};
      Any -> Any
    end.    
