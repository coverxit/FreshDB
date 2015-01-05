#pragma once

#include <string>

using namespace System::Runtime::InteropServices;

ref class MarshalHelper
{
public:
	std::string toUnmanaged(System::String^ s)
	{
		std::string ret;
		const char* c;

		c = reinterpret_cast<const char*>((Marshal::StringToHGlobalAnsi(s)).ToPointer());
		ret = c;
		Marshal::FreeHGlobal(System::IntPtr((void*)c));
		return ret;
	}

	System::String^ toManaged(std::string s)
	{
		return gcnew System::String(s.c_str());
	}
};