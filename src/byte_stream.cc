#include "byte_stream.hh"
#include <iostream>

using namespace std;

ByteStream::ByteStream( uint64_t capacity ) : capacity_( capacity ), buf_( ""), bytes_pushed_(0), bytes_popped_(0), closed_(false) {}

bool Writer::is_closed() const
{
  return closed_;
}

void Writer::push( string data )
{
  uint64_t bytes_to_push = min(available_capacity(), data.size());
  buf_.append(data.substr(0, bytes_to_push));
  bytes_pushed_ += bytes_to_push;
  return;
}

void Writer::close()
{
  closed_ = true;
  return;
}

uint64_t Writer::available_capacity() const
{
  return capacity_ - buf_.size();
}

uint64_t Writer::bytes_pushed() const
{
  return bytes_pushed_;
}

bool Reader::is_finished() const
{
  return closed_ && buf_.empty();
}

uint64_t Reader::bytes_popped() const
{
  return bytes_popped_;
}

string_view Reader::peek() const
{
  return string_view(buf_);
}

void Reader::pop( uint64_t len )
{
  uint64_t bytes_to_pop = min(len, buf_.size());
  bytes_popped_ += min(len, buf_.size());
  buf_ = buf_.substr(bytes_to_pop);
  return;
}

uint64_t Reader::bytes_buffered() const
{
  return buf_.size();
}
