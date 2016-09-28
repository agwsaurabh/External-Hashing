#include <iostream>
#include <fstream>
#include <string>
#include <cmath>
#include <vector>
#include <sstream>
#include <assert.h>
using namespace std;


string int2str(int p)
{
	ostringstream s;
 	s << p;
 	return s.str();
}

bool fexists(const string& filename) 
{
  ifstream ifile(filename.c_str());
  return ifile;
}

int hash(int val, int rnd, int no_of_buckets)
{
	int key;
	int a = 13+rnd;
	int b = 7+rnd;
	int c = 5+rnd;
	key = (a*val*val + b*val + c)%4;
	return key;
}

void readwrite(string file, int rnd, int no_of_buckets, int recrd_p_pg1, int bckt_sz[])
{
	cout << "Hashing round " << rnd << " : " << endl;
	cout << "Reading " << file << endl;

	int j=0;

	ifstream inp;
	int data;
	int key;
	inp.open(file.c_str());
	string filename;

	int * bk = new int[no_of_buckets];
	int * pg_count1 = new int[no_of_buckets];
	int ** tbl1 = new int*[no_of_buckets];

	for(int i=0; i<no_of_buckets; i++)
	{
		tbl1[i] = new int[recrd_p_pg1];
		bk[i] = 0;
		pg_count1[i] = 0;
	}

	int rel;
	int dd;

	if(file == "relation1.txt")
		rel = 1;
	if(file == "relation2.txt")
		rel = 2;
	if(file.substr(4) == "rel1")
		rel = 1;
	if(file.substr(4) == "rel2")
		rel = 2;

	while(inp >> data)
	{
		key = hash(data,rnd,recrd_p_pg1);

		cout << "Tuple " << j+1 << " : " << data << " Mapped to Bucket : " << key << endl;

		tbl1[key][bk[key]] = data;
		filename = "rel" + int2str(rel) + ".round"+ int2str(rnd) + ".bucket" + int2str(key) + ".txt";
		ofstream out;
		if(fexists(filename))
		{
			out.open(filename.c_str(),ios::app);    // open file for appending
     			assert (!out.fail( ));  
			out << tbl1[key][bk[key]] << endl;
			out.close();
			assert (!out.fail());
		}
		else
		{
			out.open(filename.c_str());
			out << tbl1[key][bk[key]] << endl;
			out.close();
		}
		bk[key]++;
		


		if(bk[key] == recrd_p_pg1)
		{
			cout << "Page for bucket " << key << " full. Flushed to Secondary Storage." << endl;
			
			bk[key] = 0;
			pg_count1[key]++;
		}

		j++;
	}

	for(int al=0; al<no_of_buckets; al++)
	{
		if(bk[al] != 0)
			pg_count1[al]++;
	}

	cout << "Done with" << file << endl;
	cout << "Created Following Files" << endl;

	for(int al=0; al<no_of_buckets; al++)
	{
		if(pg_count1[al] != 0)
			cout << "rel" << rel << ".round" << rnd << ".bucket" << al <<" : " << pg_count1[al] << "pages" << endl;

		bk[al] = 0;
			
	}
	inp.close();
	for(int al=0; al < no_of_buckets; al++)
		bckt_sz[al] = pg_count1[al];
}

void checkjoin(int bckt_sz1[], int bckt_sz2[], int max_avlpages, int rnd, int maxhashrnds, int no_of_buckets, int recrd_p_pg1, int recrd_p_pg2)
{
	string f1,f2;
	for(int i=0; i<no_of_buckets; i++)
	{
		cout << "Bucket " << i << " : Total size is " << bckt_sz2[i] + bckt_sz1 [i] << " pages" << endl;
		if(bckt_sz2[i]+bckt_sz1[i] >= max_avlpages)
		{
			if(rnd <= maxhashrnds)
			{
				cout << "In Memory Join for this Bucket can't be done" << endl;
				continue;
			}
			rnd++;
			f1 = "rel1." + "round." + int2str(rnd) + "bucket." + int2str(i);
			f2 = "rel2." + "round." + int2str(rnd) + "bucket." + int2str(i);
			readwrite(f1,1,no_of_buckets,recrd_p_pg1,bckt_sz1);
			readwrite(f2,1,no_of_buckets,recrd_p_pg2,bckt_sz2);
			checkjoin(bckt_sz1,bckt_sz2,max_avlpages,rnd,maxhashrnds,no_of_buckets,recrd_p_pg1,recrd_p_pg2);
 			
		}
		if(bckt_sz1[i]+bckt_sz2[i] < max_avlpages)
		{
			s1 = "rel1."+"round."+int2str(rnd)+"bucket."+int2str(i);
			s2 = "rel2."+"round."+int2str(rnd)+"bucket."+int2str(i);
			ifstream f1;
			int data;
			f1.open(s1.c_str());
			vector <int> f11,f12;
			while(f1 >> data)
			{
				f11.push_back(data);
			}
			f1.close();
			f1.clear();
			f1.open(s2.c_str());
			while(f1 >> data)
			{
				f12.push_back(data);
			}
			cout << "Join for bucket " << i << " ";
			for(std::vector<int>::iterator it=f11.begin(); it != f11.end(); ++it)
			{
				for(std::vector<int>::iterator jt=f12.begin(); jt != f12.end(); ++jt)
				{
					if(*it == *jt)
					{
						cout << *it << endl;
						continue;
					}
				}
			}
		}
	}
}

int main()
{
	string s1,s2,filename;
	int data;
	int recordsize1, recordsize2, pagesize, max_avlpages, maxhashrnds;

	cin >> s1 >> s2 >> recordsize1  >> recordsize2 >> pagesize >> max_avlpages >> maxhashrnds;

	ifstream inp1,inp2;
	inp1.open(s1.c_str());
	inp2.open(s2.c_str());

	int recrd_p_pg1 = pagesize/recordsize1;
	int recrd_p_pg2 = pagesize/recordsize2;
	int no_of_records1 = 0, no_of_records2 = 0;

	while(inp1>>data)
	{
		no_of_records1++;
	}

	while(inp2>>data)
	{
		no_of_records2++;
	}

	inp1.clear();
	inp2.clear();
	inp1.seekg(0, inp1.beg);
	inp2.seekg(0, inp2.beg);

	cout << endl << no_of_records1 << endl << no_of_records2 << endl;

	double size_of_reln1, size_of_reln2;

	size_of_reln1 = ceil((double)no_of_records1/(double)recrd_p_pg1);
	size_of_reln2 = ceil((double)no_of_records2/(double)recrd_p_pg2);

	cout << "Size of relation 1 : " << size_of_reln1 << " pages" << endl;
	cout << "Size of relation 2 : " << size_of_reln2 << " pages" << endl;
	cout << "Total no of available pages : " << max_avlpages << endl;

	int no_of_buckets = max_avlpages-1;

	cout << "No of buckets in Hash Table : " << no_of_buckets << endl;

	int *bckt_sz1 = new int[no_of_buckets];
	int *bckt_sz2 = new int[no_of_buckets];
	int key;

	readwrite(s1,1,no_of_buckets,recrd_p_pg1,bckt_sz1);
	readwrite(s2,1,no_of_buckets,recrd_p_pg2,bckt_sz2);
	checkjoin(bckt_sz1,bckt_sz2,max_avlpages,1,maxhashrnds,no_of_buckets,recrd_p_pg1,recrd_p_pg2);

	inp1.clear();
	inp2.clear();
	inp1.seekg(0, inp1.beg);
	inp2.seekg(0, inp2.beg);
	
	return 0;
}
