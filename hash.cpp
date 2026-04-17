#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <cstdint>

using namespace std;

// int64_t instead of long long since this is
// constant across compilers (ll gives gaurantee of atleast 64 bits)
// but could be more.
using ii = int64_t;
map<ii, ii> hash_table;

string fname = "segment";

void read() {
    return;
}

void write() {
    ii key;
    string value;
    cin >> key;
    cin.ignore(); // eat the space left after key
    getline(cin, value); // read untill \n, spaces included.
    ofstream fout(fname);
    fout << key << ";" << value << endl;
    return;
}

void update() {
    return;
}

void delete_entry() {
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
    }

}

int main (void) {
    input_loop();
    return 0;
}
