#include "reassembler.hh"
#include "byte_stream.hh"
#include <cstdint>
#include <iostream>

using namespace std;

void printMap(std::map<uint64_t, std::string> m) {
    for (const auto& [key, value] : m) {
        std::cout << key << ": " << value << ", ";
    }
    std::cout << std::endl;
}
using namespace std;

// assume second_index > first_index
// return the merged string
string RangeMap::merge_overlaping(uint64_t first_index, string first_data, uint64_t second_index, string second_data)
{
  uint64_t first_end = first_index + first_data.size() - 1;
  uint64_t second_end = second_index + second_data.size() - 1;
  if (first_end > second_end) {
    return first_data;
  }else{
    string result = first_data + second_data.substr(first_end - second_index + 1);
    return result;
  }
}

void RangeMap::insert( uint64_t first_index, string data)
{
  // Your code here.
  uint64_t last_index = first_index + data.size() - 1;

  (void)data;
  (void)last_index;
  if (data_map_.empty()) {
    data_map_[first_index] = data;
    bytes_pending_ += data.size();
    return;
  }

  // lower bound: get the smallest key greater than or equal to first_index
  auto it = data_map_.lower_bound(first_index);
  if (it != data_map_.begin() ){ // If the first key greater than or equal to first_index is begin(), there's no predecssor
    /* cout << "handling predecessor" << endl; */
    auto largest_key_before = --it; // Decrement it to get the last key less than first_index
    uint64_t largest_key_before_start = largest_key_before->first;
    uint64_t largest_key_before_end = largest_key_before->first + largest_key_before->second.size() - 1;
    /* cout << "largest key is: " << largest_key_before_start << "; end is " << largest_key_before_end << endl; */
    if (largest_key_before_end >= first_index - 1) {
      // Overlapping
      data = merge_overlaping(largest_key_before_start, largest_key_before->second, first_index, data);
      first_index = largest_key_before_start;
    }
    ++it; // increment it to go back to smallest key greater than or equal to first index
  }

  // Loop through keys greater than or equal to first_index and:
  //  - Erase anything completely covered by the new data
  //  - Merge anything overlapping/adjacent
  while (it != data_map_.end()) { 
    uint64_t curr_start = it->first;
    uint64_t curr_end = curr_start + it->second.size() - 1;
    if (curr_end < last_index){ // If we get something that ends before last_index, we can just remove it and move on to the next one
      ++it; // Note incrementing it before erasing
      data_map_.erase(curr_start);
    }else{ // Otherwise, this segment extends past last_index, so we can break after it's done
      if (curr_start  <= last_index + 1){ // If the two segments overlap, merge them and erase the original
        data = merge_overlaping(first_index, data, curr_start, it->second);
        data_map_.erase(curr_start);
      }
      break;
    }
  }
  data_map_[first_index] = data;

}

string RangeMap::get_data(uint64_t index) const
{
  auto it = data_map_.lower_bound(index);
  if (it->first == index) {
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
  return bytes_pending;
}

void Reassembler::close_if_done(){
  if (output_.writer().bytes_pushed() == final_byte_ ) {
    output_.writer().close();
  }
}

void Reassembler::insert( uint64_t first_index, string data, bool is_last_substring )
{
  uint64_t next_to_write = output_.writer().bytes_pushed();
  uint64_t capacity = output_.writer().available_capacity();
  uint64_t first_uninsertable_index = next_to_write + capacity;

  if (is_last_substring) { // If this is the last substring, set final_byte_ to the index at the end of this string
    final_byte_ = first_index + data.size();
  }

  if ( (capacity == 0) || (data.empty()) || (first_index >= first_uninsertable_index) || (first_index + data.size() <= next_to_write )) {
    close_if_done();
    return;
  }

  // Truncate the beginning of the data if it is before the next byte to write
  if (first_index < next_to_write) {
    data = data.substr(next_to_write - first_index);
    first_index = next_to_write;
  }

  // Truncate the end of the data to end at first_uninsertable_index if necessary
  data = data.substr(0, min(first_uninsertable_index - first_index,data.size()));

  // Write the data to the range_map
  data_map_.insert(first_index, data);
  string to_write = data_map_.get_data(next_to_write);
  if (!to_write.empty()) {
    uint64_t pre_bytes_pushed = output_.writer().bytes_pushed();
    output_.writer().push(to_write);
    uint64_t bytes_written = output_.writer().bytes_pushed() - pre_bytes_pushed;
    data_map_.erase(next_to_write);
    if (bytes_written < to_write.size()) {
      data_map_.insert(next_to_write + bytes_written, to_write.substr(bytes_written));
    }
  }

  close_if_done();

}

uint64_t Reassembler::bytes_pending() const
{
  return data_map_.bytes_pending();
}
