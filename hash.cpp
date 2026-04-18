#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <filesystem>

using namespace std;

// int64_t instead of long long since this is
// constant across compilers (ll gives gaurantee of atleast 64 bits)
// but could be more.
using ii = int64_t;

const ii MAX_SEGMENT_SIZE = 10;

struct RecordLocation {
    string filename;
    ii offset;
};

unordered_map<ii, RecordLocation> hash_table;
string segment_name = "segment";
int segment_number = 0;
int current_entries = 0;

string get_active_segment() {
    return segment_name + to_string(segment_number);
}

int find_latest_segment_num() {
    int latest = -1;

    for (const auto& entry : filesystem::directory_iterator(".")) {
        if (!entry.is_regular_file()) {
            continue;
        }

        string filename = entry.path().filename().string();

        if (filename.rfind(segment_name, 0) != 0) {
            continue;
        }

        string suffix = filename.substr(segment_name.size());

        if (suffix.empty()) {
            continue;
        }

        bool only_digits = true;
        for (char ch : suffix) {
            if (ch < '0' || ch > '9') {
                only_digits = false;
                break;
            }
        }

        if (!only_digits) {
            continue;
        }

        int segment_num = stoi(suffix);
        if (segment_num > latest) {
            latest = segment_num;
        }
    }

    return latest;
}

unordered_map<ii, RecordLocation> reconstruct_map() {
    unordered_map<ii, RecordLocation> temp_table;
    int latest_segment_num = find_latest_segment_num();

    if (latest_segment_num == -1) {
        segment_number = 0;
        return temp_table;
    }

    segment_number = latest_segment_num;

    for (int temp = 0; temp <= latest_segment_num; temp++) {
        string segname = segment_name + to_string(temp);
        ifstream fin(segname, ios::binary);
        if (fin) {
            string line;
            while(true) {
                ii offset = fin.tellg();
                if(!getline(fin, line)) {
                    break;
                }
                size_t pos = line.find(";");
                if (pos != string::npos) {
                    ii key = stoll(line.substr(0, pos));
                    string value = line.substr(pos+1);
                    if (value.compare("DEAD") == 0) {
                        temp_table.erase(key);
                    } else {
                        temp_table[key] = {segname, offset};
                    }
                }
            }
        }
    }

    return temp_table;
}

void compact_segments() {
    unordered_map<ii, RecordLocation> temp_table = reconstruct_map();
    string fname = "new_segment" + to_string(0);
    string compacted_fname = segment_name + to_string(0);
    ofstream fout(fname, ios::binary);
    for (auto& entry : temp_table) {
        string ifname = entry.second.filename;
        ii ioffset = entry.second.offset;
        ifstream fin(ifname, ios::binary);
        fin.seekg(ioffset, ios::beg);
        string line;
        getline(fin, line);
        ii pre_offset = fout.tellp();
        fout << line << "\n";
        hash_table[entry.first] = {compacted_fname, pre_offset};
    }
    for(int i=0;i<segment_number;i++) {
        filesystem::remove("segment" + to_string(i));
    }
    filesystem::rename(fname, "segment" + to_string(0));
    filesystem::rename(get_active_segment(), "segment" + to_string(1));
    segment_number = 1;
}

void read() {
    ii key;
    cin >> key;
    if (hash_table.find(key) != hash_table.end()) {
        RecordLocation loc = hash_table[key];
        ifstream fin(loc.filename, ios::binary);
        fin.seekg(loc.offset, ios::beg);
        string line;
        getline(fin, line);
        size_t pos = line.find(';');
        cout << line << "\n";
        cout << line.substr(pos + 1) << "\n";
    }
    return;
}

void write() {
    ii key;
    string value;
    cin >> key;
    cin.ignore(); // eat the space left after key
    getline(cin, value); // read untill \n, spaces included.

    // why binary and not text stream? text stream processes \n
    // this will mess up with the offsets.
    // Data read in from a binary stream always equal the
    // data that were earlier written out to that stream
    // https://en.cppreference.com/cpp/io/c/FILE#Binary_and_text_modes
    ofstream fout(get_active_segment(), ios::app | ios::binary);
    ii pre_offset = fout.tellp();
    fout << key << ";" << value << "\n";
    hash_table[key] = {get_active_segment(), pre_offset};
    current_entries++;
    return;
}

void update() {
    // is the same as write
    write();
    return;
}

void delete_entry() {
    ii key;
    cin >> key;
    if (hash_table.find(key) != hash_table.end()) {
        ofstream fout(get_active_segment(), ios::app | ios::binary);
        fout << key << ";" << "DEAD" << "\n";
        hash_table.erase(key);
        current_entries++;
    }
    return;
}

void input_loop(void) {

    while(true) {
        int choice;
        cout << "1. Read \n";
        cout << "2. Write \n";
        cout << "3. Update \n";
        cout << "4. Delete \n";
        cout << "5. Exit \n";
        cin >> choice;

        switch (choice) {
            case 1:
                read();
                break;
            case 2:
                write();
                break;
            case 3:
                update();
                break;
            case 4:
                delete_entry();
                break;
            case 5:
                return;
            default:
                cout << "Invalid choice \n";
        }

        if (current_entries >= MAX_SEGMENT_SIZE) {
            segment_number++;
            current_entries = 0;
        }
    }

}

int main (void) {
    compact_segments();
    input_loop();
    return 0;
}
