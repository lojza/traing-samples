#!/usr/bin/env escript

%%
%% Trace all calls from Module on Node
%%
%% Usage: 
%%
%% To trace:
%%
%%   ./mydumper.escript node@host node-cookie module
%%
%% Ctrl+C to exit
%%
%% To clear tracing
%%
%%   ./mydumper.escript node@host node-cookie clear
%%

main ([NodeStr, CookieStr | TraceOpts]) ->
    MyNode = mydumper@localhost,
    Node = list_to_atom(NodeStr),

    % connet to tohe node
    io:format(standard_error, "[~p] connecting~n", [Node]),
    {ok, _} = net_kernel:start([MyNode, shortnames]),
    if
        CookieStr /= "no_cookie" ->
            Cookie = list_to_atom(CookieStr),
            erlang:set_cookie(MyNode, Cookie);
        true -> ok
    end,
    pong = net_adm:ping(Node),
    io:format(standard_error, "[~p] connected~n", [Node]),

    Pid = self(),
    dbg:start(),
    dbg:tracer(process, {fun(Msg, N) ->
                    case Msg of
                        {trace, _, call, {M,F,_A}} ->
    		            io:format(standard_error, "[~p]      call ~p:~p~n", [N,M,F]);
                        {trace, _, return_from, {M,F,_}, _R} ->
    		            io:format(standard_error, "[~p]    return ~p:~p~n", [N,M,F]);
                        {trace, _, exception_from, {M,F,_}, _R} ->
    		            io:format(standard_error, "[~p] exception ~p:~p~n", [N,M,F]);
                        X ->
    		            io:format(standard_error, "[~p] unknow: ~p~n", [N, X])
                    end,
		    Pid ! {N, Msg},
		    N + 1
		    end, 1}),
    % clear tracing on node
    {ok, _} = dbg:n(Node),
    dbg:ctpl(),
    ok = dbg:cn(Node),

    % set tracing on node
    case TraceOpts of
        ["clear"] -> ok;
        Opts ->
            {ok, _} = dbg:n(Node),
            {ok, _} = dbg:p(all,c),
            [ begin
                  TraceMod = list_to_atom(Opt),
                  {ok, _} = dbg:tpl({TraceMod, '_', '_'}, [{'_', [], [{return_trace}, {exception_trace}]}])

              end || Opt <- Opts ],
            trace_loop()
    end.
%TraceMod /= clear ->
%        TraceMod == clear -> ok
%    end.

trace_loop() ->
    receive
	{N, Msg} ->
            dump(N, Msg),
            trace_loop()
    end.
dump(Title, Body) ->
	io:format("~n"),
	io:format("% === [ ~p ] ==================================================~n", [Title]),
	io:format("~p.~n", [Body]).
