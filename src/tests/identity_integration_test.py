import ah_config
import unittest

from modeling.model.identityrisk import checkIdentity

ah_config.initialize()


class IdentityIntegrationTest(unittest.TestCase):
    def test_identity_checkIdentity(self):
        userid = 45563
        r = checkIdentity(userid)
        assert r.toDict()['score'] == 1
