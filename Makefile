app_name := ucentral-cgw

all: output/bin/${app_name}
	@echo "uCentral CGW build app done"

clean:
	rm -rf ./output/

run: output/bin/${app_name}
	./run_cgw.sh

# TODO: replace this find with actual --out-dir for cargo
# however, as of now it's an unstable flag, hence gotta do it
# in a bash-ish way.
output/bin/${app_name}:
	@rm output/bin/${app_name} || true;
	@mkdir output/bin || true;
	@cargo build --target-dir ./output &&\
		find ./output/ -name "${app_name}" -exec cp {} ./output/bin/ -f \;
