# paths to applications
REPO_ROOT=..
ERL_LIBS:=$(REPO_ROOT):$(ERL_LIBS)

all: tar

tar:
	ERL_LIBS=$(ERL_LIBS) rebar3 as prod tar

clean:
	rm -rf _build
