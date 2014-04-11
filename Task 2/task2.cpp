#include<mpi.h>
#include<iostream>
#include<ctime>
#include<fstream>
#include<set>
#include<vector>
#include<cstdlib>
#include<algorithm>
#include<map>
using namespace std;

bool compare(pair<int, int> a, pair<int, int> b) {
    if (a.first > b.first) return false;
    return true;
}

int main(int argc, char* argv[]) {
    string line;
    int p, id;
    set<int> unique_keystore;
    vector<pair<int, int> > key_val;
    ifstream in("100000_key-value_pairs.csv");
    in >> line;
    while (in >> line) {
        string key_temp = line.substr(0, line.find(','));
        string val_temp = line.substr(line.find(',') + 1, line.length());
        int key = atoi(key_temp.c_str());
        int val = atoi(val_temp.c_str());
        pair<int, int> temp(key, val);
        key_val.push_back(temp);
        unique_keystore.insert(key);
    }
    int unique_keylength = unique_keystore.size();
    int max = (*(--unique_keystore.end()));
    int *mapper = new int[max];
    int *keyval = new int[2 * key_val.size()];
    int length = key_val.size();
    int i;
    for (i = 0; i < 2 * length; i += 2) {
        keyval[i] = key_val[i / 2].first;
        keyval[i + 1] = key_val[i / 2].second;
    }
    key_val.clear();
    unique_keystore.clear();
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &p);
    MPI_Comm_rank(MPI_COMM_WORLD, &id);
    MPI_Status status;

    int keyrange = max / (p - 1);
    //master node with node id 0 is responsible for distributing tasks to all other nodes.Also handles merging all outputs and printing it to file
    //-----------------------------------------part 1 of reduce functionality---------------------------------------------
    if (id == 0) {
        for (int i = 1; i < p; i++) {
            int temp = (2 * (i - 1) * length / (p - 1)) + (2 * length / (p - 1));
            cout<<temp;
            if(i!=p-1)
                MPI_Send(&keyval[2 * (i - 1) * length / (p - 1)], 2 * length / (p - 1), MPI_INT, i, 0, MPI_COMM_WORLD);
            else
                MPI_Send(&keyval[2 * (i - 1) * length / (p - 1)], 2*length-(2 * (i - 1) * length / (p - 1)), MPI_INT, i, 0, MPI_COMM_WORLD);
        }
        //-----------------------------------final phase of reduce functionality------------------------------------------
        vector<pair<int, int> > results;
        int count1;
        int *res = new int[20000];
        for (int j = 1; j < p; j++) {
            MPI_Recv(&res[0], 20000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_INT, &count1);
            for (int i = 0; i < count1; i += 2) {
                pair<int, int> insert_val(res[i], res[i + 1]);
                results.push_back(insert_val);
            }
        }
        sort(results.begin(), results.end(), compare);
        ofstream out("Output_Task2");
        for (int i = 0; i < results.size(); i++) {
            out << results[i].first << " " << results[i].second << "\n";
        }
    }
    if (id == 1) {
        int count1;
        int* recvdKey1 = new int[2 * length / (p - 1)];
        MPI_Recv(recvdKey1, 2 * length / (p - 1), MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        cout << "receiving data" << endl;
        MPI_Get_count(&status, MPI_INT, &count1);
        cout << "receiving data" << id << " " << recvdKey1[49999] << endl;
        vector<int> buf2;
        vector<int> buf3;
        vector<int> buf4;
        map<int, int> mapper;
        for (int i = 0; i < count1; i += 2) {
            if (recvdKey1[i] >= 0.25 * max && recvdKey1[i] < 0.50 * max) {
                buf2.push_back(recvdKey1[i]);
                buf2.push_back(recvdKey1[i + 1]);
            } else if (recvdKey1[i] >= 0.5 * max && recvdKey1[i] < 0.75 * max) {
                buf3.push_back(recvdKey1[i]);
                buf3.push_back(recvdKey1[i + 1]);
            } else if (recvdKey1[i] >= 0.75 * max) {
                buf4.push_back(recvdKey1[i]);
                buf4.push_back(recvdKey1[i + 1]);
            } else {
                if (mapper.count(recvdKey1[i]) == 0) {
                    mapper[recvdKey1[i]] = recvdKey1[i + 1];
                } else {
                    mapper[recvdKey1[i]] += recvdKey1[i + 1];
                }
            }
        }
        int* tempbuffer;
        for (int j = 1; j < id; j++) {
            if (j != id) {
                tempbuffer = new int[50000];
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.find(tempbuffer[i]) == mapper.end()) {
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        tempbuffer = new int[buf2.size()];
        copy(buf2.begin(), buf2.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf2.size(), MPI_INT, 2, 0, MPI_COMM_WORLD);
        tempbuffer = new int[buf3.size()];
        copy(buf3.begin(), buf3.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf3.size(), MPI_INT, 3, 0, MPI_COMM_WORLD);
        tempbuffer = new int[buf4.size()];
        copy(buf4.begin(), buf4.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf4.size(), MPI_INT, 4, 0, MPI_COMM_WORLD);
        cout << "sending ALL from" << id << endl;
        //------------------------------second phase of reduce for processor 1------------------------------------------
        for (int j = id; j < p; j++) {
            if (j != id) {
                tempbuffer = new int[50000];
                cout << id << " waiting for " << j << endl;
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.count(tempbuffer[i]) == 0) {
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        cout << "length1:" << mapper.size();
        int *res = new int[2 * mapper.size()];
        int i = 0;
        for (map<int, int >::const_iterator it = mapper.begin(); it != mapper.end(); ++it) {
            res[i] = it->first;
            res[i + 1] = it->second;
            i += 2;
        }
        MPI_Send(&res[0], 2 * mapper.size(), MPI_INT, 0, 0, MPI_COMM_WORLD);
    }
    if (id == 2) {

        int count3 = 0;
        int count1;
        int* recvdKey2 = new int[2 * length / (p - 1)];
        MPI_Recv(recvdKey2, 2 * length / (p - 1), MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        cout << "receiving data" << endl;
        MPI_Get_count(&status, MPI_INT, &count1);
        cout << "receiving data" << id << " " << recvdKey2[49999] << endl;
        vector<int> buf1;
        vector<int> buf3;
        vector<int> buf4;
        map<int, int> mapper;
        for (int i = 0; i < count1; i += 2) {
            if (recvdKey2[i] >= 0 * max && recvdKey2[i] < 0.25 * max) {
                buf1.push_back(recvdKey2[i]);
                buf1.push_back(recvdKey2[i + 1]);
            } else if (recvdKey2[i] >= 0.5 * max && recvdKey2[i] < 0.75 * max) {
                buf3.push_back(recvdKey2[i]);
                buf3.push_back(recvdKey2[i + 1]);
            } else if (recvdKey2[i] >= 0.75 * unique_keylength) {
                buf4.push_back(recvdKey2[i]);
                buf4.push_back(recvdKey2[i + 1]);
            } else {
                if (mapper.count(recvdKey2[i]) == 0) {
                    //count3++;

                    mapper[recvdKey2[i]] = recvdKey2[i + 1];
                } else {
                    mapper[recvdKey2[i]] += recvdKey2[i + 1];
                }
            }
        }
        int* tempbuffer;
        for (int j = 1; j < id; j++) {
            if (j != id) {
                const int t = (const int) (max / 4);
                tempbuffer = new int[50000];
                cout << id << " waiting for " << j << endl;
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.count(tempbuffer[i]) == 0) {
                        count3++;
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        tempbuffer = new int[buf1.size()];
        copy(buf1.begin(), buf1.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf1.size(), MPI_INT, 1, 0, MPI_COMM_WORLD);
        cout << "2 sent it to 1" << endl;
        tempbuffer = new int[buf3.size()];
        copy(buf3.begin(), buf3.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf3.size(), MPI_INT, 3, 0, MPI_COMM_WORLD);
        cout << "2 sent it to 3" << endl;
        tempbuffer = new int[buf4.size()];
        copy(buf4.begin(), buf4.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf4.size(), MPI_INT, 4, 0, MPI_COMM_WORLD);
        cout << "2 sent it to 3" << endl;
        //        tempbuffer = new int[unique_keylength / 4];
        //        MPI_Recv(recvdKey2, 2 * length / (p - 1), MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        cout << "sending ALL from" << id << endl;
        //------------------------------second phase of reduce for processor 2------------------------------------------
        for (int j = id; j < p; j++) {
            if (j != id) {
                tempbuffer = new int[50000];
                cout << id << " waiting for " << j << endl;
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.count(tempbuffer[i]) == 0) {
                        count3++;
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        cout << "length2:" << mapper.size();
        int *res = new int[2 * mapper.size()];
        int i = 0;
        for (map<int, int >::const_iterator it = mapper.begin(); it != mapper.end(); ++it) {
            res[i] = it->first;
            res[i + 1] = it->second;
            i += 2;
        }
        MPI_Send(&res[0], 2 * mapper.size(), MPI_INT, 0, 0, MPI_COMM_WORLD);
    }
    if (id == 3) {
        //        ofstream out("/home/questworld/Desktop/mpi exampls/sam.txt");
        int count3 = 0;
        int count1;
        int* recvdKey3 = new int[2 * length / (p - 1)];
        MPI_Recv(recvdKey3, 2 * length / (p - 1), MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        cout << "receiving data" << endl;
        MPI_Get_count(&status, MPI_INT, &count1);
        cout << "receiving data" << id << " " << recvdKey3[49999] << endl;
        vector<int> buf1;
        vector<int> buf2;
        vector<int> buf4;
        map<int, int> mapper;
        for (int i = 0; i < count1; i += 2) {
            if (recvdKey3[i] >= 0 * max && recvdKey3[i] < 0.25 * max) {
                buf1.push_back(recvdKey3[i]);
                buf1.push_back(recvdKey3[i + 1]);
            } else if (recvdKey3[i] >= 0.25 * max && recvdKey3[i] < 0.5 * max) {
                buf2.push_back(recvdKey3[i]);
                buf2.push_back(recvdKey3[i + 1]);
            } else if (recvdKey3[i] >= 0.75 * max) {
                buf4.push_back(recvdKey3[i]);
                buf4.push_back(recvdKey3[i + 1]);
            } else {
                if (mapper.count(recvdKey3[i]) == 0) {
                    mapper[recvdKey3[i]] = recvdKey3[i + 1];

                    count3++;
                } else {
                    mapper[recvdKey3[i]] += recvdKey3[i + 1];
                }
            }
        }
        int* tempbuffer;
        for (int j = 1; j < id; j++) {
            if (j != id) {
                tempbuffer = new int[50000];
                cout << id << " waiting for " << j << endl;
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.count(tempbuffer[i]) == 0) {
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                        //out<<recvdKey3[i]<<"\n";
                        //count3++;
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        tempbuffer = new int[buf1.size()];
        copy(buf1.begin(), buf1.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf1.size(), MPI_INT, 1, 0, MPI_COMM_WORLD);
        tempbuffer = new int[buf2.size()];
        copy(buf2.begin(), buf2.end(), tempbuffer);
        cout << id << " is sending messages to 2 from " << endl;
        MPI_Send(&tempbuffer[0], buf2.size(), MPI_INT, 2, 0, MPI_COMM_WORLD);
        cout << id << " has sent messages to 2 from " << endl;
        tempbuffer = new int[buf4.size()];
        copy(buf4.begin(), buf4.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf4.size(), MPI_INT, 4, 0, MPI_COMM_WORLD);
        cout << "sending ALL from" << id << endl;
        //------------------------------second phase of reduce for processor 3------------------------------------------
        for (int j = id; j < p; j++) {
            if (j != id) {
                tempbuffer = new int[50000];
                cout << id << " waiting for " << j << endl;
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.count(tempbuffer[i]) == 0) {
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                        // out<<recvdKey3[i]<<"\n";
                        //count3++;
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        cout << "length3:" << count3;
        int *res = new int[2 * mapper.size()];
        int i = 0;
        for (map<int, int >::const_iterator it = mapper.begin(); it != mapper.end(); ++it) {
            res[i] = it->first;
            res[i + 1] = it->second;
            i += 2;
        }
        MPI_Send(&res[0], 2 * mapper.size(), MPI_INT, 0, 0, MPI_COMM_WORLD);
    }
    if (id == 4) {
        int count1;
        int* recvdKey4 = new int[2 * length / (p - 1)];
        MPI_Recv(recvdKey4, 2 * length / (p - 1), MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        cout << "receiving data" << endl;
        MPI_Get_count(&status, MPI_INT, &count1);
        cout << "receiving data" << id << " " << recvdKey4[49999] << endl;
        vector<int> buf1;
        vector<int> buf2;
        vector<int> buf3;
        map<int, int> mapper;
        for (int i = 0; i < count1; i += 2) {
            if (recvdKey4[i] >= 0 * max && recvdKey4[i] < 0.25 * max) {
                buf1.push_back(recvdKey4[i]);
                buf1.push_back(recvdKey4[i + 1]);
            } else if (recvdKey4[i] >= 0.25 * max && recvdKey4[i] < 0.5 * max) {
                buf2.push_back(recvdKey4[i]);
                buf2.push_back(recvdKey4[i + 1]);
            } else if (recvdKey4[i] >= 0.5 * max && recvdKey4[i] < 0.75 * max) {
                buf3.push_back(recvdKey4[i]);
                buf3.push_back(recvdKey4[i + 1]);
            } else {
                if (mapper.count(recvdKey4[i]) == 0) {
                    mapper[recvdKey4[i]] = recvdKey4[i + 1];
                } else {
                    mapper[recvdKey4[i]] += recvdKey4[i + 1];
                }
            }
        }

        int* tempbuffer;
        for (int j = 1; j < id; j++) {
            if (j != id) {
                tempbuffer = new int[50000];
                cout << id << " waiting for " << j << endl;
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.count(tempbuffer[i]) == 0) {
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        tempbuffer = new int[buf1.size()];
        copy(buf1.begin(), buf1.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf1.size(), MPI_INT, 1, 0, MPI_COMM_WORLD);
        tempbuffer = new int[buf2.size()];
        copy(buf2.begin(), buf2.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf2.size(), MPI_INT, 2, 0, MPI_COMM_WORLD);
        tempbuffer = new int[buf3.size()];
        copy(buf3.begin(), buf3.end(), tempbuffer);
        MPI_Send(&tempbuffer[0], buf3.size(), MPI_INT, 3, 0, MPI_COMM_WORLD);
        cout << "sending ALL from" << id << endl;
        //------------------------------second phase of reduce for processor 4------------------------------------------
        for (int j = id; j < p; j++) {
            if (j != id) {
                tempbuffer = new int[50000];
                cout << id << " waiting for " << j << endl;
                MPI_Recv(&tempbuffer[0], 50000, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_INT, &count1);
                cout << id << " has received messages of length " << count1 << " from " << j << endl;
                for (int i = 0; i < count1; i += 2) {
                    if (mapper.count(tempbuffer[i]) == 0) {
                        mapper[tempbuffer[i]] = tempbuffer[i + 1];
                    } else {
                        mapper[tempbuffer[i]] += tempbuffer[i + 1];
                    }
                }
            }
        }
        cout << "length4:" << mapper.size();
        int *res = new int[2 * mapper.size()];
        int i = 0;
        for (map<int, int >::const_iterator it = mapper.begin(); it != mapper.end(); ++it) {
            res[i] = it->first;
            res[i + 1] = it->second;
            i += 2;
        }
        MPI_Send(&res[0], 2 * mapper.size(), MPI_INT, 0, 0, MPI_COMM_WORLD);
    }
    MPI_Finalize();
}
