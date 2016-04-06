CXXFLAGS += -std=c++14 -Wall -Wextra -Wpedantic -O2 -pthread
CPPFLAGS += -I"$(PWD)/vendor/concurrent_channel/include" \
            -I"$(PWD)/vendor/concurrent_channel/vendor/concurrentqueue"

bulk_uuidgen:
	$(CXX) bulk_uuidgen.cc $(CXXFLAGS) $(CPPFLAGS) -o bulk_uuidgen -Wl,-Bstatic -lboost_program_options -Wl,-Bdynamic

.PHONY: clean

clean:
	rm -Rfv ./bulk_uuidgen
