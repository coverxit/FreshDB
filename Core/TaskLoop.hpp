#pragma once

ref class TaskLoop
{
public:
	typedef KeyValuePair<bool, System::String^>										RetType;
	typedef System::Func<List<System::String^>^, List<System::String^>^, RetType>   FunctorType;

public:
	TaskLoop(BucketDictType^ _bcDict, System::String^ dbPath) : bcDict(_bcDict), databasePath(dbPath)
	{
		curBucketName = String::Empty;

		parser = new FreshCask::FQL::Parser;
		curBucket = new FreshCask::BucketManager;
		helper = gcnew MarshalHelper;

		procBegin = false;

		opDict["list bucket"] = gcnew FunctorType(this, &TaskLoop::ListBucket);
		opDict["select bucket"] = gcnew FunctorType(this, &TaskLoop::SelectBucket);
		opDict["create bucket"] = gcnew FunctorType(this, &TaskLoop::CreateBucket);
		opDict["remove bucket"] = gcnew FunctorType(this, &TaskLoop::RemoveBucket);
		opDict["get"] = gcnew FunctorType(this, &TaskLoop::Get);
		opDict["put"] = gcnew FunctorType(this, &TaskLoop::Put);
		opDict["delete"] = gcnew FunctorType(this, &TaskLoop::Delete);
		opDict["enumerate"] = gcnew FunctorType(this, &TaskLoop::Enumerate);
		opDict["compact"] = gcnew FunctorType(this, &TaskLoop::Compact);
		opDict["proc begin"] = gcnew FunctorType(this, &TaskLoop::ProcBegin);
		opDict["proc end"] = gcnew FunctorType(this, &TaskLoop::ProcEnd);
	}

	~TaskLoop()
	{
		delete parser;
		delete curBucket;
		this->!TaskLoop();
	}

	!TaskLoop() {}

public:
	RetType Parse(System::String^ q, List<System::String^>^ out)
	{
		FreshCask::FQL::Parser::ParamArray param;
		FreshCask::FQL::Parser::RetType ret = parser->Parse(helper->toUnmanaged(q), &param);
		System::String^ info = helper->toManaged(FreshCask::FQL::Parser::ToString(ret));

		if (!FreshCask::FQL::Parser::IsOK(ret))
		{
			procBegin = false; procList.Clear();
			return Fail(info);
		}
		
		FunctorType^ func;
		if (!opDict.TryGetValue(info, func))
			return Fail("unbinded identifier: `" + info + "`.");

		if (procBegin)
		{
			if (info == "proc begin")
			{
				procList.Clear(); procBegin = false;
				return Fail("No nest of `proc begin`");
			}
			else if (info != "proc end")
			{
				procList.Add(q);
				return OK("in user-defined procedure.");
			}
		}

		List<System::String^>^ args = gcnew List<System::String^>();
		for (auto& item : param)
			args->Add(helper->toManaged(item));

		out->Add(info); // add to list
		return func->Invoke(args, out);
	}

private:
	RetType ListBucket(List<System::String^>^ param, List<System::String^>^ out)
	{
		StringBuilder^ sb = gcnew StringBuilder;
		sb->Append("Bucket list:"); sb->AppendLine();
		for each (BucketItemType bc in bcDict)
		{
			out->Add(bc.Key);
			sb->Append("  " + bc.Key);
			sb->AppendLine();
		}
		sb->AppendLine();
		sb->Append("Count: " + bcDict->Count);
		return OK(sb->ToString());
	}

	RetType SelectBucket(List<System::String^>^ param, List<System::String^>^)
	{
		if (!bcDict->ContainsKey(param[0]))
			return Fail("bucket `" + param[0] + "` doesn't exist.");

		curBucket = bcDict[param[0]]; curBucketName = param[0];
		return OK("`" + param[0] + "` is now selected bucket.");
	}

	RetType CreateBucket(List<System::String^>^ param, List<System::String^>^)
	{
		if (bcDict->ContainsKey(param[0]))
			return Fail("bucket `" + param[0] + "` already exists.");

		System::String^ bcPath = Path::Combine(databasePath, param[0]);
		try
		{
			Directory::CreateDirectory(bcPath);
		}
		catch (Exception^ e)
		{
			return Fail(e->Message + Environment::NewLine + e->StackTrace);
		}

		FreshCask::BucketManager *bc = new FreshCask::BucketManager;
		FreshCask::Status s = bc->Open(helper->toUnmanaged(bcPath));
		if (!s)
			return Fail("failed to open bucket `" + param[0] + "`, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()));
		else
			bcDict->Add(param[0], BucketManagerWrapper(bc));

		curBucket = bcDict[param[0]]; curBucketName = param[0];
		return OK("`" + param[0] + "` is created and selected as current bucket.");
	}

	RetType RemoveBucket(List<System::String^>^ param, List<System::String^>^)
	{
		if (!bcDict->ContainsKey(param[0]))
			return Fail("bucket `" + param[0] + "` doesn't exist.");

		FreshCask::BucketManager* bc = bcDict[param[0]];
		FreshCask::Status s = bc->Close();
		if (!s)
			return Fail("failed to close bucket `" + param[0] + "`, detail information:" + Environment::NewLine + helper->toManaged(s.ToString()));

		try 
		{
			System::String^ bcPath = Path::Combine(databasePath, param[0]);
			Directory::Delete(bcPath, true);
		}
		catch (Exception ^e)
		{
			return Fail(e->Message + Environment::NewLine + e->StackTrace);
		}

		if (curBucketName == param[0])
			curBucket = nullptr, curBucketName = String::Empty;

		bcDict->Remove(param[0]);
		return OK("`" + param[0] + "` is deleted.");
	}

	RetType Get(List<System::String^>^ param, List<System::String^>^ out)
	{
		if (curBucketName == String::Empty)
			return Fail("there is no bucket selected.");

		FreshCask::SmartByteArray value;
		FreshCask::Status s = curBucket->Get(helper->toUnmanaged(param[0]), value);
		if (!s)
			return Fail(helper->toManaged(s.ToString()));

		out->Add(helper->toManaged(value.ToString()));
		return OK("Value: " + helper->toManaged(value.ToString()));
	}

	RetType Put(List<System::String^>^ param, List<System::String^>^)
	{
		if (curBucketName == String::Empty)
			return Fail("there is no bucket selected.");

		FreshCask::Status s = curBucket->Put(helper->toUnmanaged(param[0]), helper->toUnmanaged(param[1]));
		if (!s)
			return Fail(helper->toManaged(s.ToString()));

		return OK("The operation is completed successfully");
	}

	RetType Delete(List<System::String^>^ param, List<System::String^>^)
	{
		if (curBucketName == String::Empty)
			return Fail("there is no bucket selected.");

		FreshCask::Status s = curBucket->Delete(helper->toUnmanaged(param[0]));
		if (!s)
			return Fail(helper->toManaged(s.ToString()));

		return OK("The operation is completed successfully");
	}

	RetType Enumerate(List<System::String^>^ param, List<System::String^>^ out)
	{
		if (curBucketName == String::Empty)
			return Fail("there is no bucket selected.");

		std::vector<std::string> keys;
		FreshCask::Status s = curBucket->Enumerate(keys);
		if (!s)
			return Fail(helper->toManaged(s.ToString()));

		for (auto& key : keys)
			out->Add(helper->toManaged(key));

		return OK("Count: " + keys.size() + " records in `" + curBucketName + "`.");
	}

	RetType Compact(List<System::String^>^ param, List<System::String^>^)
	{
		if (curBucketName == String::Empty)
			return Fail("there is no bucket selected.");

		FreshCask::Status s = curBucket->Compact();
		if (!s)
			return Fail(helper->toManaged(s.ToString()));

		return OK("The operation is completed successfully");
	}

	RetType ProcBegin(List<System::String^>^ param, List<System::String^>^)
	{
		if (procBegin)
		{
			procList.Clear(); procBegin = false;
			return Fail("No nest of `proc begin`");
		}

		procBegin = true;
		return OK();
	}

	RetType ProcEnd(List<System::String^>^ param, List<System::String^>^ out)
	{
		if (!procBegin)
			return Fail("No `proc begin` before.");

		for each (System::String^ op in procList)
			out->Add(op);

		procBegin = false;
		procList.Clear();
		return OK();
	}

public:
	FreshCask::BucketManager* GetCurrentBucket() { return curBucket; }
	System::String^ GetCurrentBucketName() { return curBucketName; }
	bool IsProcBegin() { return procBegin; }
	
public:
	static bool IsOK(RetType ret) { return ret.Key; }
	static System::String^ ToString(RetType ret) { return ret.Value; }

private:
	static RetType OK() { return OK(""); }
	static RetType OK(System::String^ msg) { return RetType(true, msg); }
	static RetType Fail(System::String^ msg) { return RetType(false, msg); }

private:
	FreshCask::FQL::Parser *parser;
	FreshCask::BucketManager *curBucket;
	System::String^ curBucketName;
	MarshalHelper^ helper;
	Dictionary<System::String^, FunctorType^> opDict;

	bool procBegin;
	List<System::String^> procList;

	System::String^ databasePath;
	BucketDictType^ bcDict;
};