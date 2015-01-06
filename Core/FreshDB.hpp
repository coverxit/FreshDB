#pragma once

#include <memory>

using namespace System;
using namespace System::IO;
using namespace System::Net;
using namespace System::Text;
using namespace System::Threading;
using namespace System::Reflection;
using namespace System::Net::Sockets;
using namespace System::Threading::Tasks;
using namespace System::Collections::Generic;

#include <Util/MarshalHelper.hpp>
#include <Util/MD5Helper.hpp>

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
	const int bufferSize = 8192;
	System::String^ const adminBucketName = "__admin_bucket__";
	System::String^ const promptSymbol    = "freshdb> ";
	System::String^ const promptContinue  = "    ...> ";

public:
	FreshDB(System::String^ _databasePath, IPAddress^ _listenInterface, unsigned short _listenPort)
	{
		databasePath = _databasePath;
		listenInterface = _listenInterface;
		listenPort = _listenPort;

		logger = gcnew StreamWriter("freshdb.log", true);
		logMutex = gcnew Object;
		bcDict = gcnew BucketDictType;
	}

	~FreshDB()
	{
		this->!FreshDB();
	}

	!FreshDB() 
	{
		try
		{
			logger->Close();
		}
		catch (Exception^)
		{
			
		}
	}

public:
	static void PrintWelcome()
	{
		Console::WriteLine("FreshDB version {0}, build on {1} at {2}", Assembly::GetExecutingAssembly()->GetName()->Version, __DATE__, __TIME__);
		Console::WriteLine("Enter `.help` for instructions");
		Console::WriteLine("Enter `.login` to login");
		Console::WriteLine("Enter FQL statements after logging in");
		Console::WriteLine("");
	}

	static void PrintCommandUsage()
	{
		Console::WriteLine(".admin [OP] <IN>  Manage DB admin where [OP] is one of:");
		Console::WriteLine("                    list    List DB admins");
		Console::WriteLine("                    add     Add a DB admin");
		Console::WriteLine("                    delete  Delete a DB admin");
		Console::WriteLine("                    modify  Modify password of a DB admin");
		Console::WriteLine(".login            Log in");
		Console::WriteLine(".logout           Log out");
		Console::WriteLine(".help             Show this message");
		Console::WriteLine(".quit             Exit this program");
	}

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
				Log("admin bucket `" + adminBucketName + "` doesn't exist, try to create");
				Directory::CreateDirectory(adminBCPath);
				Log("created admin bucket at `" + adminBCPath + "` successfully.");
			}

			MarshalHelper^ helper = gcnew MarshalHelper();
			adminBC = new FreshCask::BucketManager;

			FreshCask::Status s = adminBC->Open(helper->toUnmanaged(adminBCPath));
			if (!s) 
				Log("failed to open bucket `" + adminBucketName + "`, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()), "Error");
			else
			{
				if (adminBC->PairCount() == 0)
					if (!InitAdminBC()) return;

				array<System::String^>^ dirs = Directory::GetDirectories(databasePath);
				for each (System::String^% dir in dirs)
				{
					System::String^ bcName = Path::GetFileName(dir);
					if (bcName == adminBucketName) continue;

					FreshCask::BucketManager *bc = new FreshCask::BucketManager;
					FreshCask::Status s = bc->Open(helper->toUnmanaged(dir));
					if (!s)
						Log("failed to open bucket `" + bcName + "`, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()), "Error");
					else
						bcDict->Add(bcName, BucketManagerWrapper(bc));
				}

				netTask = gcnew Task(gcnew Action<System::Object^>(this, &FreshDB::RemoteLoop), TaskCreationOptions::LongRunning | TaskCreationOptions::PreferFairness);
				netTask->Start();
				LocalLoop();

				Log("Quitting... ");
				Cleanup();
			}
		}
		catch (Exception^ e)
		{
			Log(e->Message + Environment::NewLine + e->StackTrace);
		}
	}

	void Cleanup()
	{
		for each (BucketItemType bc in bcDict)
		{
			bc.Value->Close();
			delete bc.Value.get();
		}

		if (adminBC)
		{
			adminBC->Close();
			delete adminBC;
		}
	}

private:
	bool InitAdminBC()
	{
		Log("no admin found in `" +adminBucketName + "`, create one:");

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
				Log("failed to create admin account, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()), "Error");
				return false;
			}
			else
			{
				adminBC->Flush();
				Log("create admin account successfully.");
				return true;
			}
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
			TaskLoop^ loop = gcnew TaskLoop(bcDict, databasePath);
			System::String^ input;
			MarshalHelper^ helper = gcnew MarshalHelper;
			bool logged = false;

			while (true)
			{
				if (loop->IsProcBegin()) Console::Write(promptContinue);
				else Console::Write(promptSymbol); 
				
				input = Console::ReadLine();
				if (input == nullptr) break;

				if (input->StartsWith("."))
				{
					if (input == ".quit")
						break;
					else if (input == ".help")
						PrintCommandUsage();
					else if (input->StartsWith(".login"))
					{
						if (logged)
						{
							Log("you have already logged in.", "Error");
							continue;
						}

						array<Char>^ delim = { ' ' };
						array<String^>^ cmds = input->Split(delim);

						if (cmds->Length == 1)
						{
							Log("syntax: .login [username] [password]");
							continue;
						}

						try
						{
							System::String ^username = cmds[1];
							System::String ^password = cmds[2];

							if (!adminBC->CotainsKey(helper->toUnmanaged(username)))
								Log("wrong password or username doesn't exist.", "Error");
							else
							{
								FreshCask::SmartByteArray value;
								FreshCask::Status s = adminBC->Get(helper->toUnmanaged(username), value);
								if (!s)
									Log(helper->toManaged(s.ToString()), "Error");
								else
								{
									MD5Helper^ md5 = gcnew MD5Helper;
									if (md5->Get(password) == helper->toManaged(value.ToString()))
										logged = true, Log("login successfully, " + username + ".");
									else
										Log("wrong password or username doesn't exist.", "Error");
								}
							}
						}
						catch (Exception^)
						{
							Log("syntax: .login [username] [password]");
						}
					}
					else if (input == ".logout")
					{
						if (!logged)
						{
							Log("you haven't logged in.", "Error");
							continue;
						}

						logged = false;
						Log("logout successfully.");
					}
					else if (input->StartsWith(".admin"))
					{
						if (!logged)
						{
							Log("you haven't logged in.", "Error");
							continue;
						}

						array<Char>^ delim = { ' ' };
						array<String^>^ cmds = input->Split(delim);

						if (cmds->Length == 1)
						{
							Log("invalid argument: `" + input + "`. Enter `.help` for help.", "Error");
							continue;
						}

						if (cmds[1] == "list")
						{
							if (adminBC->PairCount() == 0)
								Log("No admin accounts.");
							else
							{
								std::vector<std::string> admins;
								FreshCask::Status s = adminBC->Enumerate(admins);
								if (!s)
									Log(helper->toManaged(s.ToString()), "Error");
								else
								{
									Log("Admin list:");
									for (auto& admin : admins)
									{
										FreshCask::SmartByteArray value;
										FreshCask::Status s = adminBC->Get(admin, value);
										if (!s)
											Log(helper->toManaged(s.ToString()), "Error");
										else
											Log("  " + helper->toManaged(admin));
									}

									Log("Count: " + admins.size());
								}
							}
						}
						else if (cmds[1] == "add")
						{
							try
							{
								System::String ^username = cmds[2];
								System::String ^password = cmds[3];

								if (adminBC->CotainsKey(helper->toUnmanaged(username)))
									Log("admin `" + username + "` already exists.", "Error");
								else
								{
									MD5Helper^ md5 = gcnew MD5Helper;
									password = md5->Get(password);

									FreshCask::SmartByteArray value;
									FreshCask::Status s = adminBC->Put(helper->toUnmanaged(username), helper->toUnmanaged(password));
									if (!s)
										Log(helper->toManaged(s.ToString()), "Error");
									else
										Log("add admin `" + username + "` successfully.");
								}
							}
							catch (Exception^)
							{
								Log("syntax: .admin add [username] [password]");
							}
						}
						else if (cmds[1] == "modify")
						{
							try
							{
								System::String ^username = cmds[2];
								System::String ^password = cmds[3];

								if (!adminBC->CotainsKey(helper->toUnmanaged(username)))
									Log("admin `" + username + "` dosen't exists.", "Error");
								else
								{
									MD5Helper^ md5 = gcnew MD5Helper;
									password = md5->Get(password);

									FreshCask::SmartByteArray value;
									FreshCask::Status s = adminBC->Put(helper->toUnmanaged(username), helper->toUnmanaged(password));
									if (!s)
										Log(helper->toManaged(s.ToString()), "Error");
									else
										Log("modify password of admin `" + username + "` successfully.");
								}
							}
							catch (Exception^)
							{
								Log("syntax: .admin modify [username] [password]");
							}
						}
						else if (cmds[1] == "delete")
						{
							try
							{
								System::String ^username = cmds[2];

								if (!adminBC->CotainsKey(helper->toUnmanaged(username)))
									Log("admin `" + username + "` dosen't exists.", "Error");
								else
								{
									FreshCask::SmartByteArray value;
									FreshCask::Status s = adminBC->Delete(helper->toUnmanaged(username));
									if (!s)
										Log(helper->toManaged(s.ToString()), "Error");
									else
										Log("delete admin `" + username + "` successfully.");
								}
							}
							catch (Exception^)
							{
								Log("syntax: .admin delete [username]");
							}
						}
						else
							Log("invalid argument: `" + cmds[1] + "`. Enter `.help` for help.", "Error");
					}
					else
						Log("unknown command: `" + input + "`. Enter `.help` for help.", "Error");
				}
				else 
				{
					if (!logged)
					{
						Log("you haven't logged in.", "Error");
						continue;
					}

					List<System::String^>^ param = gcnew List<System::String^>;
					TaskLoop::RetType ret = loop->Parse(input, param);

					if (!TaskLoop::IsOK(ret))
						Log(TaskLoop::ToString(ret), "Error");
					else
					{
						if (loop->IsProcBegin()) continue;

						if (param[0] == "enumerate")
						{
							param->RemoveAt(0);
							if (param->Count > 0) // has keys
							{	
								FreshCask::BucketManager *bc = loop->GetCurrentBucket();
								for each (System::String^% key in param)
								{
									FreshCask::SmartByteArray value;
									FreshCask::Status s = bc->Get(helper->toUnmanaged(key), value);
									if (!s)
									{
										Log(helper->toManaged(s.ToString()), "Error");  
										break;
									}
									else
										Log("Key: " + key + ", Value: " + helper->toManaged(value.ToString()));
								}
							}
							Log("");
							Log(TaskLoop::ToString(ret));
						}
						else if (param[0] == "list bucket")
						{
							if (param->Count > 1)
								Log(TaskLoop::ToString(ret));
							else
								Log("there is no buckets.");
						}
						else if (param[0] == "proc end")
						{
							param->RemoveAt(0);
							if (param->Count > 0)
							{
								bool success = true;
								for each (System::String^% st in param)
								{
									List<System::String^>^ tmp = gcnew List<System::String^>;
									TaskLoop::RetType ret = loop->Parse(st, tmp);
									if (!TaskLoop::IsOK(ret))
									{
										Log("when executing statement: `" + st->Trim() + "`", "Error");
										Log(TaskLoop::ToString(ret), "Error");
										success = false; break;
									}
								}
								if (success)
									Log("procedure execution finished successfully.");
							}
						}
						else
							Log(TaskLoop::ToString(ret));
					}
				}
			}
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
				Log("server started at " + listenInterface->Address.ToString() + " on " + listenPort.ToString() + ".", "Remote");
			else
				Log("server listened on " + listenPort.ToString() + ".", "Remote");

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
		Log("incoming connection from " + ip->Address->ToString(), "Remote");

		ASCIIEncoding^ encoder = gcnew ASCIIEncoding();

		TaskLoop^ loop = gcnew TaskLoop(bcDict, databasePath);
		MarshalHelper^ helper = gcnew MarshalHelper;
		bool logged = false;

		while (true)
		{
			try
			{
				NetworkStream^ stream = cliSock->GetStream();
			
				array<Byte>^ buffer = gcnew array<Byte>(bufferSize);
				int bytesReaded = stream->Read(buffer, 0, buffer->Length);
				
				System::String^ data = encoder->GetString(buffer, 0, bytesReaded);
				array<System::String^>^ cmds = data->Split(Environment::NewLine->ToCharArray());

				System::String^ servResponse = System::String::Empty;

				for each (System::String^% cmd in cmds)
				{
					if (cmd->StartsWith("login"))
					{
						array<Char>^ delim = { ' ' };
						array<String^>^ param = cmd->Split(delim);

						if (param->Length == 1)
						{
							servResponse = "0syntax: .login [username] [password]";
							goto sendResponse;
						}

						try
						{
							System::String ^username = param[1];
							System::String ^password = param[2];

							if (!adminBC->CotainsKey(helper->toUnmanaged(username)))
								servResponse = "0wrong password or username doesn't exist.";
							else
							{
								FreshCask::SmartByteArray value;
								FreshCask::Status s = adminBC->Get(helper->toUnmanaged(username), value);
								if (!s)
									servResponse = "0" + helper->toManaged(s.ToString());
								else
								{
									MD5Helper^ md5 = gcnew MD5Helper;
									if (md5->Get(password) == helper->toManaged(value.ToString()))
									{
										logged = true;
										servResponse = "1login successfully, " + username + ".";
									}
									else
										servResponse = "0wrong password or username doesn't exist.";
								}
							}
						}
						catch (Exception^)
						{
							servResponse = "0syntax: .login [username] [password]";
						}
					}
					else
					{
						if (!logged)
						{
							servResponse = "0you haven't logged in.";
							goto sendResponse;
						}

						List<System::String^>^ param = gcnew List < System::String^ > ;
						TaskLoop::RetType ret = loop->Parse(cmd, param);

						if (!TaskLoop::IsOK(ret))
							servResponse = "0" + TaskLoop::ToString(ret);
						else
						{
							if (loop->IsProcBegin()) continue;

							if (param[0] == "enumerate")
							{
								servResponse = "1";

								param->RemoveAt(0);
								if (param->Count > 0) // has keys
								{
									FreshCask::BucketManager *bc = loop->GetCurrentBucket();
									for each (System::String^% key in param)
									{
										FreshCask::SmartByteArray value;
										FreshCask::Status s = bc->Get(helper->toUnmanaged(key), value);
										if (!s)
										{
											servResponse = "0" + helper->toManaged(s.ToString());
											break;
										}
										else
											servResponse += key + ":" + helper->toManaged(value.ToString()) + Environment::NewLine;
									}
								}
							}
							else if (param[0] == "list bucket")
							{
								servResponse = "1";

								param->RemoveAt(0);
								if (param->Count > 0) // has buckets
								{
									for each (System::String^% bucket in param)
										servResponse += bucket + Environment::NewLine;
								}
							}
							else if (param[0] == "proc end")
							{
								param->RemoveAt(0);
								if (param->Count > 0)
								{
									bool success = true;
									for each (System::String^% st in param)
									{
										List<System::String^>^ tmp = gcnew List<System::String^>;
										TaskLoop::RetType ret = loop->Parse(st, tmp);
										if (!TaskLoop::IsOK(ret))
										{
											servResponse = "when executing statement: `" + st->Trim() + "`" + Environment::NewLine;
											servResponse += TaskLoop::ToString(ret);
											success = false; break;
										}
									}

									if (success)
										servResponse = "2procedure execution finished successfully.";
								}
							}
							else if (param[0] == "get")
								servResponse = "1" + param[1]; // param[1] - value
							else
								servResponse = "1" + TaskLoop::ToString(ret);
						}
					}
				}
				
sendResponse:
				array<Byte>^ sendBytes = encoder->GetBytes(servResponse);
				stream->Write(sendBytes, 0, sendBytes->Length);
				stream->Flush();
			}
			catch (Exception^ e)
			{
				Log(e->Message + Environment::NewLine + e->StackTrace);
			}
		}
	}

private:
	void Log(String^ msg) { Log(msg, "", false); }
	void Log(String^ msg, String ^prefix) { Log(msg, prefix, false); }
	void Log(String^ msg, String^ prefix, bool fileOnly)
	{
		Monitor::Enter(logMutex);

		System::String ^consolePrefix, ^filePrefix;
		
		if (prefix != String::Empty)
		{
			consolePrefix = prefix + ": ";
			filePrefix = prefix + ": ";
		}
		else
		{
			consolePrefix = String::Empty;
			filePrefix = "Info: ";
		}

		if (!fileOnly) Console::WriteLine("{0}{1}", consolePrefix, msg);
		logger->WriteLine("[{0} {1}] {2}{3}", DateTime::Now.ToLongDateString(), DateTime::Now.ToLongTimeString(), filePrefix, msg);

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
