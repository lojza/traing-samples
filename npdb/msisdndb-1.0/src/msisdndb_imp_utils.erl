%%
%% Imporotvaci funkce
%%
%% Jak nejrychleji nacitat soubory.
%% Nejrychlejsi je to pomoci file:read_file nacist cele do pameti a pak
%% to pomoci binary:split rozebirat na jednotlive radky (postupne, ne najednou
%% s option global) a to pak dale rozebirat pomoci split a ;
%%
%% Jednoltive binarni kousky neprevated na string (list) ale rovnou parsovat.
%%
%% Proti cteni po jednotlivych radku (i kdyz je zde raw a read_ahead) to je asi 3x rychlejsi
%% (1.1s proti 3.2s)
%%
%% Velkym zdrenim (30s) je rozebirat string regularnim vyrazem.
%% Dalsim urychlenim je cpat data do databaze asynchronne (insert_async) pomoci gen_server:cast.
%% O neco malo rychlejsi je nacist data do pole, z nej pak nekde jinde vytvorit spravne struktury,
%% najednou je vsechny zvalidovat a pak to do tabulky vrazit jednim ets:insert. Zvladne to i
%% miliony dat.
%%
%% Po techto optimalizacich to naloaduje NPDB soubor (1.7 milionu cisel) za  cca 9.5s.
%% Pri cteni po radcich to trva 16.5s.
%%
-module (msisdndb_imp_utils).

-export([read_file/3, read_file/2]).

read_file (FileName, Callable) ->
  read_file (FileName, Callable, []).

read_file(FileName, Fun, Acc0) when is_function (Fun) ->
  case process_file (FileName, {'fun', Fun, Acc0}) of
    {ok, {'fun', _, LastAcc}} -> {ok, LastAcc};
    {error, Reason} -> {error, Reason}
  end;

read_file(FileName, Module, Opts) when is_atom(Module) ->
  {ok, State} = Module:init(Opts),
  case process_file (FileName, {mod, Module, State}) of
    {ok, {mod, _, FinalState}} -> Module:make_result(FinalState);
    {error, Reason} -> {error, Reason}
  end.

process_file(FileName, ProcData) ->
  case file:read_file(FileName) of
    {ok, BinData} -> read_lines (BinData, ProcData);
    {error, Reason} -> {error, Reason}
  end.

read_lines (BinData, ProcData) ->
  LFPatt = binary:compile_pattern([<<"\n">>]),
  split_lines (BinData, LFPatt, ProcData).

split_lines (BinData, LFPatt, ProcData) ->
  split_lines (BinData, LFPatt, ProcData, 1).

split_lines (BinData, LFPatt, ProcData, LineNum) ->
  case binary:split (BinData, LFPatt) of
    [Line, TailBinData] -> 
      case process_line (Line, LineNum, ProcData) of
        {ok, NewProcData} -> split_lines  (TailBinData, LFPatt, NewProcData, LineNum + 1);
        {error, Reason} -> {error, {line_error, LineNum, Reason}}
      end;
    [LastLine] when LastLine /= <<>> -> 
      case process_line (LastLine, LineNum, ProcData) of
        {ok, LastProcData} -> {ok, LastProcData};
        {error, Reason} -> {error, {line_error, LineNum, Reason}}
      end;		  
    [<<>>] -> {ok, ProcData}
  end.

process_line (Line, LineNum, ProcData) ->
  case ProcData of
    {'fun', Fun, Acc} -> 
      case Fun (Line, LineNum, Acc) of
        ok -> {ok, {'fun', Fun, Acc}};
	{ok, NewAcc} -> {ok, {'fun', Fun, NewAcc}};
        {error, Reason} -> {error, Reason}
      end;
    {mod, Module, State} ->
      case Module:process_line(Line, LineNum, State) of
        ok -> {ok, {mod, Module, State}};
	{ok, NewState} -> {ok, {mod, Module, NewState}};
        {error, Reason} -> {error, Reason}	      
      end
  end.
