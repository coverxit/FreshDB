#pragma once

#include <memory>

using namespace System;
using namespace System::IO;
using namespace System::Net;
using namespace System::Net::Sockets;
using namespace System::Threading;
using namespace System::Threading::Tasks;
using namespace System::Collections::Generic;

#include <FreshCask.h>

#include <Util/MarshalHelper.hpp>
#include <Util/MD5Helper.hpp>

#include <Core/TaskLoop.hpp>

value class BucketManagerWrapper
{
public:
	BucketManagerWrapper(FreshCask::BucketManager* _bc) : bc(_bc) {}
	operator FreshCask::BucketManager*() { return get(); }
	FreshCask::BucketManager* operator->() { return get(); }
	FreshCask::BucketManager* get() { return bc; }

private:
	FreshCask::BucketManager *bc;
};

typedef KeyValuePair<System::String^, BucketManagerWrapper> BucketItemType;
typedef Dictionary<System::String^, BucketManagerWrapper> BucketDictType;
#include <Core/TaskLoop.hpp>

ref class FreshDB
{
private:
	const int bufferSize = 4096;
	System::String^ const adminBucketName = "__admin_bucket__";

public:
	FreshDB(System::String^ _databasePath, IPAddress^ _listenInterface, unsigned short _listenPort)
	{
		databasePath = _databasePath;
		listenInterface = _listenInterface;
		listenPort = _listenPort;
		logger = gcnew StreamWriter("freshdb.log", true);
		logMutex = gcnew Object;
	}

	~FreshDB()
	{
		logger->Close();

		for each (BucketItemType bc in bcDict)
		{
			bc.Value->Close();
			delete bc.Value.get();
		}

		if (adminBC)
			delete adminBC;

		this->!FreshDB();
	}

	!FreshDB() {}

public:
	void Run()
	{
		try
		{
			if (!Directory::Exists(databasePath))
			{
				Log("`" + databasePath + "` doesn't exist, try to create.");
				Directory::CreateDirectory(databasePath);
				Log("created database at `" + databasePath + "` successfully.");
			}

			System::String^ adminBCPath = Path::Combine(databasePath, adminBucketName);
			if (!Directory::Exists(adminBCPath))
			{
				Log("Admin bucket `" + adminBucketName + "` doesn't exist, try to create");
				Directory::CreateDirectory(adminBucketName);
				Log("created admin bucket at `" + adminBucketName + "` successfully.");
			}

			MarshalHelper^ helper = gcnew MarshalHelper();
			adminBC = new FreshCask::BucketManager;

			FreshCask::Status s = adminBC->Open(helper->toUnmanaged(adminBCPath));
			if (!s) 
				Log("Failed to open bucket `" + adminBucketName + "`, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()));
			else
			{
				if (adminBC->PairCount() == 0)
					if (!InitAdminBC()) return;

				array<System::String^>^ dirs = Directory::GetDirectories(databasePath);
				for each (System::String^% dir in dirs)
				{
					System::String^ bcName = Path::GetDirectoryName(dir);
					
					FreshCask::BucketManager *bc = new FreshCask::BucketManager;
					FreshCask::Status s = bc->Open(helper->toUnmanaged(bcName));
					if (!s)
						Log("Failed to open bucket `" + bcName + "`, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()));
					else
						bcDict->Add(bcName, BucketManagerWrapper(bc));
				}

				netTask = gcnew Task(gcnew Action<System::Object^>(this, &FreshDB::RemoteLoop), TaskCreationOptions::LongRunning | TaskCreationOptions::PreferFairness);
				LocalLoop();
			}
		}
		catch (Exception^ e)
		{
			Log(e->Message + Environment::NewLine + e->StackTrace);
		}
	}

public:
	bool InitAdminBC()
	{
		Log("No admin found in `" +adminBucketName + "`, create one:");

		try
		{
			System::String ^username, ^password;
			Console::Write("Username: "); 
			username = Console::ReadLine();
			Console::Write("Password: "); 
			password = Console::ReadLine();

			MD5Helper^ md5 = gcnew MD5Helper;
			password = md5->Get(password);

			MarshalHelper^ helper = gcnew MarshalHelper;
			FreshCask::Status s = adminBC->Put(helper->toUnmanaged(username), helper->toUnmanaged(password));

			if (!s)
			{
				Log("Failed to open create admin, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()));
				return false;
			}
			else
				return true;
		}
		catch (Exception^ e)
		{
			Log(e->Message + Environment::NewLine + e->StackTrace);
			return false;
		}
	}

	void LocalLoop()
	{
		try
		{
			TaskLoop^ loop = gcnew TaskLoop;
			System::String^ input;

			while ((input = Console::ReadLine()) != nullptr)
				loop->Parse(input);
		}
		catch (Exception^ e)
		{
			Log(e->Message + Environment::NewLine + e->StackTrace);
		}
	}

	void RemoteLoop(System::Object^ param)
	{
		try
		{
			TcpListener^ serSock = gcnew TcpListener(listenInterface, listenPort);
			TcpClient^ cliSock = gcnew TcpClient();

			serSock->Start();
			if (listenInterface != IPAddress::Any)
				Log("Server started at " + listenInterface->Address.ToString() + " on " + listenPort.ToString() + ".");
			else
				Log("Server listened on " + listenPort.ToString() + ".");

			while (true)
			{
				cliSock = serSock->AcceptTcpClient();
				cliFactory.StartNew(gcnew Action<System::Object^>(this, &FreshDB::HandleConnection), cliSock, TaskCreationOptions::LongRunning | TaskCreationOptions::PreferFairness);
			}
		}
		catch (Exception^ e)
		{
			Log(e->Message + Environment::NewLine + e->StackTrace);
		}
	}

	void HandleConnection(System::Object^ param)
	{
		TcpClient^ cliSock = (TcpClient^)param;

		IPEndPoint^ ip = (IPEndPoint^)cliSock->Client->RemoteEndPoint;
		Log("Incoming connection from " + ip->Address->ToString());

		while (true)
		{

		}
	}

private:
	void Log(String^ msg) { Log(msg, false); }
	void Log(String^ msg, bool fileOnly)
	{
		Monitor::Enter(logMutex);

		if (!fileOnly) Console::WriteLine("freshdb: {0}", msg);
		logger->WriteLine("[{0} {1}] {2}", DateTime::Now.ToLongDateString(), DateTime::Now.ToLongTimeString(), msg);

		Monitor::Exit(logMutex);
	}

private:
	FreshCask::BucketManager* adminBC = nullptr;
	BucketDictType^ bcDict;

	Task^ netTask;
	TaskFactory cliFactory;

	IPAddress^ listenInterface;
	unsigned short listenPort; // FC
	System::String^ databasePath;

	StreamWriter^ logger;
	System::Object^ logMutex;
};
