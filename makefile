all:
#	g++ -Wno-unused-result -ansi -pedantic -Wall -B /usr/local/share/libhugetlbfs/ -Wl,--hugetlbfs-align -O3 -fomit-frame-pointer -o naskitis_non_commercial_ozsort_stage_one-hugeTLB naskitis_non_commercial_ozsort_stage_one.c -lrt
#	g++ -Wno-unused-result -ansi -pedantic -Wall -B /usr/local/share/libhugetlbfs/ -Wl,--hugetlbfs-align -O3 -fomit-frame-pointer -o naskitis_non_commercial_ozsort_stage_two-hugeTLB naskitis_non_commercial_ozsort_stage_two.c -lrt
	g++ -w -Wno-unused-result -ansi -pedantic -Wall -O3 -fomit-frame-pointer -o naskitis_non_commercial_ozsort_stage_one naskitis_non_commercial_ozsort_stage_one.c -lrt -lpthread
	g++ -w -Wno-unused-result -ansi -pedantic -Wall -O3 -fomit-frame-pointer -o naskitis_non_commercial_ozsort_stage_two naskitis_non_commercial_ozsort_stage_two.c -lrt -lpthread
	@cat USAGE_POLICY.txt;
clean:
	rm -rf naskitis_non_commercial_ozsort_stage_one naskitis_non_commercial_ozsort_stage_one-hugeTLB naskitis_non_commercial_ozsort_stage_two naskitis_non_commercial_ozsort_stage_two-hugeTLB
