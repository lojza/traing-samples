-record(user, {
          id              :: integer(),
          login           :: term(),
          enabled  = true :: boolean(),
          features        :: ordict:orddict(atom(), term())
         }).
