#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <cstdint>

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
    input_loop();
    return 0;
}
