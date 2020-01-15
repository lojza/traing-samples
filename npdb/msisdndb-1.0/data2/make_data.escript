#!/usr/bin/env escript

%%
%% random data generator (not optimized algorithm, please be patient)
%%

-compile([{nowarn_deprecated_function, {random, uniform, 1}}]).

main([FileType, CountStr]) ->
    Count = list_to_integer(CountStr),
    Intervals0 = case FileType of
                     "rdc" -> not_overlapping_random_intervals(FileType, 200000000, 999000000, 1, 100, Count);
                     "fix" -> not_overlapping_random_intervals(FileType, 200000000, 599000000, 1, 5, Count);
                     "mob" -> not_overlapping_random_intervals(FileType, 600000000, 799000000, 1, 1, Count);
                     "extra" -> not_overlapping_random_intervals(FileType, 600000000, 799000000, 1, 5, Count)
                 end,

    % put here random oper Id
    [ begin
        OperId = 200 + random:uniform(100),
        case FileType of
            "rdc" -> io:format(";~p;~p;;;;~p;;~n", [From, To, OperId]);
            "fix" -> io:format("~p;~p;~p;~p;~n", [From, To, OperId, OperId]);
            "mob" -> io:format("~p;;~p;~n", [From, OperId]);
            "extra" -> io:format("~p;~p;~p;~n", [From, To, OperId])
        end
      end || {From, To} <- Intervals0],
    ok.

not_overlapping_random_intervals(FileType, From, To, MinInterval, MaxInterval, Count) ->
    TabId = ets:new(undefined, [ordered_set, {keypos, 1}]),
    not_overlapping_random_intervals_loop(FileType, From, To, MinInterval, MaxInterval, Count, 0, 0, TabId),
    Intervals = ets:tab2list(TabId),
    ets:delete(TabId),
    Intervals.

not_overlapping_random_intervals_loop(_FileType, _From, _To, _MinInterval, _MaxInterval, Count, _Total, N, _TabId) when N > 100 * Count ->
    % stop, too many loops
    exit ("too many attempts");
not_overlapping_random_intervals_loop(_FileType, _From, _To, _MinInterval, _MaxInterval, Count, Total, _N, _TabId) when Count == Total -> ok;
not_overlapping_random_intervals_loop(FileType, From, To, MinInterval, MaxInterval, Count, Total, N, TabId) ->
    Interval = random_interval (From, To, MinInterval, MaxInterval),
    {IFrom, ITo} = Interval,

    PrevOk = case ets:prev(TabId, IFrom) of
                 IPrevFrom when is_integer(IPrevFrom) ->
                     [{IPrevFrom, IPrevTo}] = ets:lookup(TabId, IPrevFrom),
                     IPrevTo < IFrom;
                 '$end_of_table' -> true
             end,

    NextOk = case ets:next(TabId, IFrom) of
                 INextFrom when is_integer(INextFrom) ->
                     [{INextFrom, _INextTo}] = ets:lookup(TabId, INextFrom),
                     ITo < INextFrom;
                 '$end_of_table' -> true
             end,

    if 
        PrevOk and NextOk ->
            ets:insert(TabId, Interval),
            if (Total rem 10000 == 0) -> io:format(standard_error, "~s ~p/~p (~p)~n", [FileType, Total, Count, N - Total]);
               true -> ok
            end,
            not_overlapping_random_intervals_loop(FileType, From, To, MinInterval, MaxInterval, Count, Total + 1, N + 1, TabId);
        true ->
            not_overlapping_random_intervals_loop(FileType, From, To, MinInterval, MaxInterval, Count, Total, N + 1, TabId)
    end.


random_interval (From, To, MinInterval, MaxInterval) ->
    Begin = From + random:uniform(To - From + 1),
    End = Begin + MinInterval + random:uniform(MaxInterval - MinInterval + 1) - 1,
    {Begin, End}.

