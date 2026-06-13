#include "file_server.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

using namespace messageSystem;

class FileServiceTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        initLogger("test", "", spdlog::level::debug);
    }

    void SetUp() override {
        test_dir_ = "/tmp/file_service_test_" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) + "/";
        service_ = std::make_unique<FileServiceImpl>(test_dir_);
    }

    void TearDown() override {
        service_.reset();
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
    }

    std::string test_dir_;
    std::unique_ptr<FileServiceImpl> service_;
};

TEST_F(FileServiceTest, PutSingleFile) {
    PutFileReq req;
    req.set_request_id("req-001");
    auto *file_data = req.add_file_data();
    file_data->set_file_name("test.txt");
    file_data->set_file_size(13);
    file_data->set_file_content("Hello, World!");

    PutFileRsp rsp;
    service_->PutSingleFile(nullptr, &req, &rsp, nullptr);

    EXPECT_TRUE(rsp.success());
    EXPECT_EQ(rsp.request_id(), "req-001");
    ASSERT_EQ(rsp.file_info_size(), 1);
    EXPECT_FALSE(rsp.file_info(0).file_id().empty());
    EXPECT_EQ(rsp.file_info(0).file_size(), 13);
    EXPECT_EQ(rsp.file_info(0).file_name(), "test.txt");

    std::string fid = rsp.file_info(0).file_id();
    std::string full_path = test_dir_ + fid;
    EXPECT_TRUE(std::filesystem::exists(full_path));

    std::ifstream ifs(full_path, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                         std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "Hello, World!");
}

TEST_F(FileServiceTest, PutMultiFile) {
    PutFileReq req;
    req.set_request_id("req-002");

    auto *f1 = req.add_file_data();
    f1->set_file_name("a.txt");
    f1->set_file_size(5);
    f1->set_file_content("AAAAA");

    auto *f2 = req.add_file_data();
    f2->set_file_name("b.txt");
    f2->set_file_size(5);
    f2->set_file_content("BBBBB");

    auto *f3 = req.add_file_data();
    f3->set_file_name("c.txt");
    f3->set_file_size(5);
    f3->set_file_content("CCCCC");

    PutFileRsp rsp;
    service_->PutMultiFile(nullptr, &req, &rsp, nullptr);

    EXPECT_TRUE(rsp.success());
    EXPECT_EQ(rsp.request_id(), "req-002");
    ASSERT_EQ(rsp.file_info_size(), 3);

    EXPECT_EQ(rsp.file_info(0).file_name(), "a.txt");
    EXPECT_EQ(rsp.file_info(1).file_name(), "b.txt");
    EXPECT_EQ(rsp.file_info(2).file_name(), "c.txt");

    for (int i = 0; i < 3; i++) {
        EXPECT_FALSE(rsp.file_info(i).file_id().empty());
        EXPECT_EQ(rsp.file_info(i).file_size(), 5);
        std::string path = test_dir_ + rsp.file_info(i).file_id();
        EXPECT_TRUE(std::filesystem::exists(path));
    }

    std::string content_a, content_b, content_c;
    fileUtil::FileSystem fs;
    fs.read(test_dir_, rsp.file_info(0).file_id(), content_a);
    fs.read(test_dir_, rsp.file_info(1).file_id(), content_b);
    fs.read(test_dir_, rsp.file_info(2).file_id(), content_c);

    EXPECT_EQ(content_a, "AAAAA");
    EXPECT_EQ(content_b, "BBBBB");
    EXPECT_EQ(content_c, "CCCCC");
}

TEST_F(FileServiceTest, GetSingleFile) {
    PutFileReq put_req;
    put_req.set_request_id("put-001");
    auto *file_data = put_req.add_file_data();
    file_data->set_file_name("download.txt");
    file_data->set_file_size(11);
    file_data->set_file_content("Test content");

    PutFileRsp put_rsp;
    service_->PutSingleFile(nullptr, &put_req, &put_rsp, nullptr);
    ASSERT_TRUE(put_rsp.success());
    std::string fid = put_rsp.file_info(0).file_id();

    GetFileReq get_req;
    get_req.set_request_id("get-001");
    get_req.add_file_id_list(fid);

    GetFileRsp get_rsp;
    service_->GetSingleFile(nullptr, &get_req, &get_rsp, nullptr);

    EXPECT_TRUE(get_rsp.success());
    EXPECT_EQ(get_rsp.request_id(), "get-001");
    EXPECT_EQ(get_rsp.file_data_size(), 1);
    EXPECT_TRUE(get_rsp.file_data().count(fid));
    EXPECT_EQ(get_rsp.file_data().at(fid).file_content(), "Test content");
    EXPECT_EQ(get_rsp.file_data().at(fid).file_id(), fid);
}

TEST_F(FileServiceTest, GetMultiFile) {
    PutFileReq put_req;
    put_req.set_request_id("put-002");

    auto *f1 = put_req.add_file_data();
    f1->set_file_name("x.txt");
    f1->set_file_size(1);
    f1->set_file_content("X");

    auto *f2 = put_req.add_file_data();
    f2->set_file_name("y.txt");
    f2->set_file_size(1);
    f2->set_file_content("Y");

    PutFileRsp put_rsp;
    service_->PutMultiFile(nullptr, &put_req, &put_rsp, nullptr);
    ASSERT_TRUE(put_rsp.success());
    ASSERT_EQ(put_rsp.file_info_size(), 2);

    std::string fid1 = put_rsp.file_info(0).file_id();
    std::string fid2 = put_rsp.file_info(1).file_id();

    GetFileReq get_req;
    get_req.set_request_id("get-002");
    get_req.add_file_id_list(fid1);
    get_req.add_file_id_list(fid2);

    GetFileRsp get_rsp;
    service_->GetMultiFile(nullptr, &get_req, &get_rsp, nullptr);

    EXPECT_TRUE(get_rsp.success());
    EXPECT_EQ(get_rsp.file_data_size(), 2);
    EXPECT_EQ(get_rsp.file_data().at(fid1).file_content(), "X");
    EXPECT_EQ(get_rsp.file_data().at(fid2).file_content(), "Y");
}

TEST_F(FileServiceTest, GetNonExistentFile) {
    GetFileReq req;
    req.set_request_id("get-404");
    req.add_file_id_list("non_existent_file_id");

    GetFileRsp rsp;
    service_->GetSingleFile(nullptr, &req, &rsp, nullptr);

    EXPECT_FALSE(rsp.success());
    EXPECT_FALSE(rsp.errmsg().empty());
}

TEST_F(FileServiceTest, PutThenGetRoundTrip) {
    std::string large_content(2 * 1024 * 1024, 'Z');

    PutFileReq put_req;
    put_req.set_request_id("put-large");
    auto *file_data = put_req.add_file_data();
    file_data->set_file_name("large.bin");
    file_data->set_file_size(large_content.size());
    file_data->set_file_content(large_content);

    PutFileRsp put_rsp;
    service_->PutSingleFile(nullptr, &put_req, &put_rsp, nullptr);
    ASSERT_TRUE(put_rsp.success());
    std::string fid = put_rsp.file_info(0).file_id();

    GetFileReq get_req;
    get_req.set_request_id("get-large");
    get_req.add_file_id_list(fid);

    GetFileRsp get_rsp;
    service_->GetSingleFile(nullptr, &get_req, &get_rsp, nullptr);

    EXPECT_TRUE(get_rsp.success());
    EXPECT_EQ(get_rsp.file_data().at(fid).file_content().size(), large_content.size());
    EXPECT_EQ(get_rsp.file_data().at(fid).file_content(), large_content);
}

TEST_F(FileServiceTest, FileIdsAreUnique) {
    PutFileReq req1;
    req1.set_request_id("u1");
    req1.add_file_data()->set_file_content("data1");

    PutFileReq req2;
    req2.set_request_id("u2");
    req2.add_file_data()->set_file_content("data2");

    PutFileRsp rsp1, rsp2;
    service_->PutSingleFile(nullptr, &req1, &rsp1, nullptr);
    service_->PutSingleFile(nullptr, &req2, &rsp2, nullptr);

    ASSERT_TRUE(rsp1.success());
    ASSERT_TRUE(rsp2.success());
    EXPECT_NE(rsp1.file_info(0).file_id(), rsp2.file_info(0).file_id());
}

TEST_F(FileServiceTest, MultiFileIdsAreAllUnique) {
    PutFileReq req;
    req.set_request_id("mu");
    for (int i = 0; i < 5; i++) {
        req.add_file_data()->set_file_content("content" + std::to_string(i));
    }

    PutFileRsp rsp;
    service_->PutMultiFile(nullptr, &req, &rsp, nullptr);

    ASSERT_TRUE(rsp.success());
    ASSERT_EQ(rsp.file_info_size(), 5);

    std::set<std::string> ids;
    for (int i = 0; i < 5; i++) {
        ids.insert(rsp.file_info(i).file_id());
    }
    EXPECT_EQ(ids.size(), 5);
}
