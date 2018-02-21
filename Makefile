all: mbtiles

.PHONY:mbtiles
mbtiles:
	jbuilder build src/top/top.exe
	_build/default/src/top/top.exe

.PHONY:clean
clean:
	jbuilder clean

install:
	jbuilder build @install
	jbuilder install
