#include <string>
#include <stdlib.h>
#include <mp/wavy.h>
#include <mp/sync.h>
#include <unordered_set>
#include <msgpack.hpp>
#include "hash64.h"
#include "hash32.h"
#include "random64.h"
#include "tcp_wrap.h"
#include "address.hpp"
#include "sockets.hpp"
#include "merdy_operations.h"
#include <limits.h>
#include "debug_mode.h"
#include "dynamo_objects.hpp"
#include "mercury_objects.hpp"

int main(void)
{
	attr a(std::string("a"));
	attr b(std::string("b"));
	
	attr c(std::string("c"));

	assert(a < b);
	assert(b < c);
}
