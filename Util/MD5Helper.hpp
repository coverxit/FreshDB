#pragma once

using namespace System::Text;
using namespace System::Security::Cryptography;

ref class MD5Helper
{
public:
	System::String^ Get(System::String^ str)
	{
		ASCIIEncoding^ encoder = gcnew ASCIIEncoding();

		MD5^ md5 = gcnew MD5CryptoServiceProvider;
		array<Byte>^ hash = md5->ComputeHash(encoder->GetBytes(str));

		StringBuilder^ sb = gcnew StringBuilder;
		for each (Byte b in hash)
			sb->Append(b.ToString("x2"));

		return sb->ToString();
	}
};
