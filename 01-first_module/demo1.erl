%
% simple example
%
-module(demo1).

-export([sum/2, sum_and_avg/2]).

sum(A, B) -> A + B.

sum_and_avg (A, B) ->
    Sum = A + B,
    Avg = Sum / 2,
    {Sum, Avg}.
