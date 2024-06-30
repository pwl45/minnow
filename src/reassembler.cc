#include "reassembler.hh"
#include "byte_stream.hh"
#include <cstdint>
#include <iostream>
/* #include "../tests/common.hh" */

using namespace std;

void printMap(std::map<uint64_t, std::string> m) {
    for (const auto& [key, value] : m) {
        /* std::cout << key << ": " << Printer::prettify(value) << ", "; */
        std::cout << key << ": " << value << ", ";
    }
    std::cout << std::endl;
}
using namespace std;

// assume second_index > first_index
// return the merged string
string RangeMap::merge_overlaping(uint64_t first_index, string first_data, uint64_t second_index, string second_data)
{
  /* cout << "merging " << Printer::prettify(first_data) << " at " << first_index << " with " << Printer::prettify(second_data) << " at " << second_index << endl; */
  uint64_t first_end = first_index + first_data.size() - 1;
  uint64_t second_end = second_index + second_data.size() - 1;
  if (first_end > second_end) {
    return first_data;
  }else{
    string result = first_data + second_data.substr(first_end - second_index + 1);
    /* cout << "merged " << result << " at " << first_index << endl; */
    return result;
  }
}

void RangeMap::insert( uint64_t first_index, string data)
{
  // Your code here.
  /* cout << "Inserting " << Printer::prettify(data) << " at " << first_index << "(size= " << data.size() << endl; */
  /* printMap(data_map_); */
  uint64_t last_index = first_index + data.size() - 1;

  (void)data;
  (void)last_index;
  if (data_map_.empty()) {
    /* cout << "data map empty... simple insert" << endl; */
    data_map_[first_index] = data;
    bytes_pending_ += data.size();
    /* cout << "returning..."; */
    return;
  }

  auto it = data_map_.lower_bound(first_index);
  if (it == data_map_.begin() ){
  /* if (it == data_map_.begin()) { */
    /* cout << "no predecessor" << endl; */
    cout << (it == data_map_.begin()) << endl;
    /* cout << (first_index) << endl; */
    cout << (it->first + it->second.size()) << endl;
  }else{
    cout << "handling predecessor" << endl;
    auto largest_key_before = --it;
    uint64_t largest_key_before_start = largest_key_before->first;
    uint64_t largest_key_before_end = largest_key_before->first + largest_key_before->second.size() - 1;
    /* cout << "largest key is: " << largest_key_before_start << "; end is " << largest_key_before_end << endl; */
    if (largest_key_before_end >= first_index - 1) {
      // Overlapping
      data = merge_overlaping(largest_key_before_start, largest_key_before->second, first_index, data);
      first_index = largest_key_before_start;
    }
  }
  it = data_map_.lower_bound(first_index);
  while (it != data_map_.end()) {
    uint64_t curr_start = it->first;
    uint64_t curr_end = curr_start + it->second.size() - 1;
    /* cout << "handling data from " << curr_start << " to " << curr_end << endl; */
    if (curr_start > last_index + 1) {
      break;
    }else{
      if (curr_end < last_index){
        // current is completely covered by data - remove current from datamap
        /* bytes_pending_ -= it->second.size(); */
        /* cout << "current is completely covered by data" << endl; */
        ++it;
        data_map_.erase(curr_start);
        /* cout << "erased " << curr_start << endl; */
        /* printMap(data_map_); */
        continue;
      }else{
        /* cout << "current is partially covered by data or adjacent" << endl; */
        // current is partially covered by data - merge the two strings and remove current
        data = merge_overlaping(first_index, data, curr_start, it->second);
        uint64_t prev_size = it->second.size();
        data_map_.erase(curr_start);
        bytes_pending_ += data.size() - prev_size;
        break;
      }
    }
    ++it;
  }
  /* cout << "inserting " << Printer::prettify(data) << " at " << first_index << "(size=" << data.size() << ")" << endl; */
  data_map_[first_index] = data;
  /* cout << "inserted " << Printer::prettify(data) << " at " << first_index << "(size=" << data.size() << ")" << endl; */
  /* printMap(data_map_); */

}

string RangeMap::get_data(uint64_t index) const
{
  auto it = data_map_.lower_bound(index);
  if (it->first == index) {
    /* cout << "returning " << it->second << " at " << it->first << endl; */
    return it->second;
  }else{
    return "";
  }
}


void RangeMap::erase( uint64_t key)
{
  data_map_.erase(key);
}

uint64_t RangeMap::bytes_pending() const
{
  // sum the sizes of all the strings in data_map_
  uint64_t bytes_pending = 0;
  for (const auto& [_, data] : data_map_) {
    bytes_pending += data.size();
  }
  /* cout << "bytes_pending: " << bytes_pending << endl; */
  return bytes_pending;
}

void Reassembler::insert( uint64_t first_index, string data, bool is_last_substring )
{
  // Your code here.
  (void)first_index;
  (void)data;
  (void)is_last_substring;
  /* cout << "Starting insert of " << Printer::prettify(data) << " at " << first_index << "; size=" << data.size() << endl; */

  if (is_last_substring) {
    final_byte_ = first_index + data.size();
  }
  uint64_t next_to_write = output_.writer().bytes_pushed();
  uint64_t max_insertable_index = next_to_write + output_.writer().available_capacity();
  uint64_t last_index = first_index + data.size() - 1;

  if (first_index >= max_insertable_index) {
    if (output_.writer().bytes_pushed() == final_byte_ ) {
      output_.writer().close();
    }
    return;
  }
  // Truncate the beginning of the data if it is before the next byte to write
  /* cout << "ntw: " << next_to_write << "; fi: " << first_index << endl; */
  if (first_index < next_to_write) {
      uint64_t overlap = next_to_write - first_index;
      /* cout << "truncating beginning of data by " << overlap << " bytes" << endl; */ 
      if (overlap >= data.size()) {
          data = "";
          first_index = next_to_write;
      } else {
          data = data.substr(next_to_write - first_index);
          first_index = next_to_write;
      }
  }

  // Truncate the end of the data if it is after the last byte to write
  if (last_index >= max_insertable_index) {
      data = data.substr(0, max_insertable_index - first_index);
  }

  // Write the data to the range_map
  data_map_.insert(first_index, data);
  string to_write = data_map_.get_data(next_to_write);
  if (!to_write.empty()) {
    uint64_t bytes_to_write = to_write.size();
    uint64_t pre_bytes_pushed = output_.writer().bytes_pushed();
    output_.writer().push(to_write);
    uint64_t post_bytes_pushed = output_.writer().bytes_pushed();
    uint64_t bytes_written = post_bytes_pushed - pre_bytes_pushed;
    data_map_.erase(next_to_write);
    if (bytes_written < bytes_to_write) {
      data_map_.insert(next_to_write + bytes_written, to_write.substr(bytes_written));
    }
  }
  /* cout << "bytes pushed: " << output_.writer().bytes_pushed() << endl; */

  if (output_.writer().bytes_pushed() == final_byte_ ) {
    output_.writer().close();
  }

}

uint64_t Reassembler::bytes_pending() const
{
  return data_map_.bytes_pending();
  return {};
}
