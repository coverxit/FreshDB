#pragma once

ref class TaskLoop
{
public:
	TaskLoop()
	{
		parser = new FreshCask::FQL::Parser;
		curBucket = new FreshCask::BucketManager;
	}

	~TaskLoop()
	{
		delete parser;
		delete curBucket;
		this->!TaskLoop();
	}

	!TaskLoop() {}

public:
	bool Parse(System::String^ q) 
	{
		MarshalHelper^ helper = gcnew MarshalHelper;

		parser->Parse(helper->toUnmanaged(q));
	}

private:
	FreshCask::FQL::Parser *parser;
	FreshCask::BucketManager *curBucket;
};