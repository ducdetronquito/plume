from factory import (
    Factory, Faker, fuzzy, post_generation, SelfAttribute, SubFactory
)


class MetaProfile(Factory):
    class Meta:
        model = dict

    mastodon_profile = SelfAttribute('..name')
    mastodon_followers = fuzzy.FuzzyInteger(0, 100)

    @post_generation
    def add_social_media_profile(self, create, extracted, **kwargs):
        user_name = self['mastodon_profile'].replace(' ', '').lower()
        self['mastodon_profile'] = user_name + '@mastodon.fr'


class Persona(Factory):
    class Meta:
        model = dict

    name = Faker('name')
    age = fuzzy.FuzzyInteger(0, 100)
    size = fuzzy.FuzzyFloat(1.50, 2.0)
    meta = SubFactory(MetaProfile)
