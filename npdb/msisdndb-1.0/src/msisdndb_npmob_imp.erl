%%
%% NPDB MOB imort module
%%

-module(msisdndb_npmob_imp).

-include("msisdndb_logging.hrl").

-export([read_file/2, read_file/1]).
-export([init/1, process_line/3, make_result/1]).
-record(st, {
  split_patt,
  counter,
  values
}).

-define(IMORT_BATCH_SIZE, 100000).

read_file (FileName) ->
    read_file (FileName, []).

read_file (FileName, Opts) ->
    msisdndb_imp_utils:read_file (FileName, ?MODULE).

init(_Opts) -> 
   St = #st{
     counter = 0,
     split_patt = binary:compile_pattern([<<";">>]),
     values = []
   },
   {ok, St}.

% skip first line with a header
process_line (_, 1, _ ) -> ok;

% process line
process_line (BinLine, _LineNum, #st{counter = N, split_patt = Patt, values = Values} = St) ->
    if (N rem 100000 == 0) and (N >0) -> ?LOG_INFO ("NPMOB readed records: ~p", [N]);
       true -> ok
    end, 

    Fields = binary:split(BinLine, Patt, [global]),
    Msisdn = erlang:binary_to_integer(lists:nth(1,Fields)),
    OpId = erlang:binary_to_integer(lists:nth(3,Fields)),
    Data = {Msisdn, OpId},
    
    {ok, St#st{counter = N + 1, values = [Data | Values]}}.

make_result(#st{counter = N, values = Values}) ->
    ?LOG_INFO ("NPMOB import records: ~p", [N]),
    insert_loop (Values, ?IMORT_BATCH_SIZE).

insert_loop (List, N) -> insert_loop (List, N, 0).

insert_loop ([], _N, Acc) -> Acc;
insert_loop (List, N, Acc) ->
    {Batch, Tail} = head_n(List, N),
    true = msisdndb_npmob:insert_many (Batch),
    Count = length (Batch),
    NewAcc = Acc + Count,
    ?LOG_INFO ("imported records: ~p", [NewAcc]),
    insert_loop (Tail, N, NewAcc).

head_n (List, N) ->
    head_n (List, N, []).

head_n ([H | T], N, Acc) when N > 0 ->
%io:format("~p~n", [N]),
    head_n (T, N - 1, [H | Acc]);
head_n (T, 0, Acc) -> {lists:reverse(Acc), T};
head_n ([], _N, Acc) -> {lists:reverse(Acc), []}.
