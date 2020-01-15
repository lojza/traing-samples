%%
%% logging macros - if you want connect it to some logging framework, do it here
%%

-ifndef (MSISDNDB_LOGGING).
-define (MSISDNDB_LOGGING, ok).

% connet to logging framework here
-define (LOG_MSG(Logger, Level, Str, Args), io:format("[~w] [~w] [~w] " ++ Str ++ " ~n", [Logger, self(), Level] ++ Args)).

-define (LOG_MSG(Level, Str, Args), ?LOG_MSG(?MODULE, Level, Str, Args)).

% user interfaces
-define (LOG_DBG      (Str, Args), ?LOG_MSG (debug,    Str, Args)).
-define (LOG_INFO     (Str, Args), ?LOG_MSG (info,     Str, Args)).
-define (LOG_NOTICE   (Str, Args), ?LOG_MSG (notice,   Str, Args)).
-define (LOG_WARN     (Str, Args), ?LOG_MSG (warn,     Str, Args)).
-define (LOG_ERROR    (Str, Args), ?LOG_MSG (error,    Str, Args)).
-define (LOG_ALERT    (Str, Args), ?LOG_MSG (alert,    Str, Args)).
-define (LOG_CRITICAL (Str, Args), ?LOG_MSG (critical, Str, Args)).

-define (LOG_DBG      (Str), ?LOG_DBG     (Str, [])).
-define (LOG_INFO     (Str), ?LOG_INFO    (Str, [])).
-define (LOG_NOTICE   (Str), ?LOG_NOTICE  (Str, [])).
-define (LOG_WARN     (Str), ?LOG_WARN    (Str, [])).
-define (LOG_ERROR    (Str), ?LOG_ERROR   (Str, [])).
-define (LOG_ALERT    (Str), ?LOG_ALERT   (Str, [])).

-endif.

