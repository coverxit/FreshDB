// FreshDB.cpp: 主项目文件。

#include "stdafx.h"

#include <Core/FreshDB.hpp>

void PrintUsage()
{
	Console::WriteLine("OVERVIEW: FreshCask Database Server");
	Console::WriteLine();
	Console::WriteLine("USAGE: freshdb [options] <inputs>");
	Console::WriteLine();
	Console::WriteLine("OPTIONS:");
	Console::WriteLine("  -d, --dbpath <path>       Path to database (required)");
	Console::WriteLine("  -l, --listen <address>    Local address to listen on (optional, any)");
	Console::WriteLine("  -p, --port <port>         Local port to listen on (optional, 17222)");
	Console::WriteLine();
}

value struct test
{
	FreshCask::BucketManager *bc;

	test(FreshCask::BucketManager *b) : bc(b) {}
};

int main(array<System::String ^> ^args)
{
	IPAddress^ listenInterface = IPAddress::Any;
	unsigned short listenPort = 17222; // FC
	System::String^ databasePath;
	
	if (args->Length < 2) // at lease --dbpath
	{
		PrintUsage();
		return 0;
	}

	if (args->Length > 0)
	{
		for (int i = 0; i < args->Length; i++)
		{
			try
			{
				if (args[i][0] == '-')
				{
					String^ opt;

					if (args[i][1] == '-')
						opt = args[i]->Substring(2);
					else
						opt = args[i]->Substring(1);

					if (opt == "d" || opt == "dbpath")
						databasePath = args[++i];
					else if (opt == "l" || opt == "listen")
						listenInterface = IPAddress::Parse(args[++i]);
					else if (opt == "p" || opt == "port")
						listenPort = UInt16::Parse(args[++i]);
					else
					{
						PrintUsage();
						return 0;
					}
				}
			}
			catch (Exception^ e)
			{
				Console::WriteLine("freshdb: {0}{1}{2}", e->Message, Environment::NewLine, e->StackTrace);
				Console::WriteLine("Try `freshdb --help` for more information.");
				return 0;
			}
		}
	}

	try
	{
		FreshDB^ dbServ = gcnew FreshDB(databasePath, listenInterface, listenPort);
		dbServ->Run();
	}
	catch (Exception^ e)
	{
		Console::WriteLine("freshdb: {0}{1}{2}", e->Message, Environment::NewLine, e->StackTrace);
		Console::WriteLine("Try `freshdb --help` for more information.");
		return 0;
	}

	return 0;
}
