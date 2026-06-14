#include <gtest/gtest.h>
#include <string>
#include <cctype>

namespace messageSystem {
    bool checkPassword(const std::string& str) {
        if(str.size() < 6 || str.size() > 40)
            return false;
        for(const auto & it : str) {
            if(!std::isalnum(it) && it != '-' && it != '_')
                return false;
        }
        return true;
    }

    bool checkName(const std::string& name) {
        return name.size() < 10;
    }
}

using namespace messageSystem;

TEST(UserServiceTest, CheckPasswordValid) {
    EXPECT_TRUE(checkPassword("abc123"));
    EXPECT_TRUE(checkPassword("test-user_123"));
    EXPECT_TRUE(checkPassword("123456"));
    EXPECT_TRUE(checkPassword("abcdef"));
}

TEST(UserServiceTest, CheckPasswordTooShort) {
    EXPECT_FALSE(checkPassword("short"));
    EXPECT_FALSE(checkPassword("12345"));
}

TEST(UserServiceTest, CheckPasswordTooLong) {
    EXPECT_FALSE(checkPassword("this_password_is_way_way_way_way_too_long_for_the_limit"));
}

TEST(UserServiceTest, CheckPasswordInvalidChars) {
    EXPECT_FALSE(checkPassword("has space"));
    EXPECT_FALSE(checkPassword("has@special"));
    EXPECT_FALSE(checkPassword("has.dot"));
    EXPECT_FALSE(checkPassword("has+plus"));
}

TEST(UserServiceTest, CheckNameValid) {
    EXPECT_TRUE(checkName("abc"));
    EXPECT_TRUE(checkName("123456789"));
    EXPECT_TRUE(checkName("a"));
    EXPECT_TRUE(checkName("user-name"));
}

TEST(UserServiceTest, CheckNameTooLong) {
    EXPECT_FALSE(checkName("this_name_is_way_too_long"));
    EXPECT_FALSE(checkName("1234567890"));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
