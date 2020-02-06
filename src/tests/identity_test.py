import unittest
import ah_config

from modeling.model.identityrisk import checkzip, usernameType

ah_config.initialize()


class IdentityTest(unittest.TestCase):
    def test_identity_checkzip(self):
        user1 = dict()
        user2 = dict()
        user1['userid'] = 45563
        user2['userid'] = 45563
        assert checkzip(user1, user2) == 1

    def test_identity_usernameType(self):
        username1 = '77061688802:08:45 AM'
        username2 = '7706168880'
        username3 = 'test.username@gmail.com'
        username4 = 'IntuitUsbankTest'

        assert usernameType(username1) == 1
        assert usernameType(username2) == 1
        assert usernameType(username3) == 0
        assert usernameType(username4) == 2

