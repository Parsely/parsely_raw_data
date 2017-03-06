from __future__ import absolute_import, print_function, division

__license__ = """
Copyright 2016 Parsely, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
LINE_DELIMITER = '\x02\n\x03'


class SlotsMixin(object):
    """Mixin to handle __eq__ and __repr__ when using __slots__."""
    __slots__ = ()  # Needs to be here, or inheriting class will get a __dict__

    def _get_inherited_slots(self):
        """Return a sequence of properties on this object inherited from __slots__"""
        # __slots__ is not inherited from the superclass when accessed as self.__slots__
        # work around this here by manually inspecting the __slots__ or everything
        # on the MRO
        mro_slots = [item for slots in
                     [a.__slots__ for a in type(self).mro() if hasattr(a, "__slots__")]
                     for item in slots]
        seen = set()
        deduplicated_slots = []
        for item in mro_slots:
            if item in seen:
                continue
            seen.add(item)
            deduplicated_slots.append(item)
        return (p for p in deduplicated_slots if not p.startswith('_'))

    def __eq__(self, other):
        """Compare and return True if all public attributes are equal."""
        if not isinstance(other, self.__class__):
            return False
        return all(getattr(self, p) == getattr(other, p)
                   for p in self._get_inherited_slots())

    def __repr__(self):
        """Basic __repr__ which prints a dict of what's in __slots__."""
        clsname = self.__class__.__name__
        vals = ', '.join('{}={!r}'.format(s, getattr(self, s, None))
                         for s in self._get_inherited_slots())
        output = "{}({})".format(clsname, vals)
        return output


class EventFlags(SlotsMixin):
    """Class representing flags for an event.

    Flags are meant to be information that comes across in the raw
    event that would otherwise be lost after parsing. For example,
    AMP events have `ampid` on the request URL. This is used to populate
    the site id, and then the name `ampid` is discarded. To track AMP
    events, we use the `is_amp` flag.

    This should not be used for information that otherwise exists on an
    event. For example, `is_mobile` would be bad, since that can already
    be determined by looking at the User-Agent.
    """
    __slots__ = ('is_amp',)
    __version__ = 1

    def __init__(self, is_amp):
        self.is_amp = is_amp

class Metadata(SlotsMixin):
    """Class representing in-pixel metadata.

    This is implemented using __slots__ to be as efficient as possible.
    """
    __slots__ = ('authors', 'canonical_url', 'urls', 'page_type', 'post_id',
                 'pub_date_tmsp', 'custom_metadata', 'section', 'tags',
                 'save_date_tmsp', 'thumb_url', 'title', 'image_url',
                 'full_content_word_count', 'share_urls', 'duration',
                 'data_source')
    __version__ = 1

    def __init__(self, authors, canonical_url, urls, page_type, post_id,
                 pub_date_tmsp, custom_metadata, section, tags,
                 save_date_tmsp, thumb_url, title, image_url,
                 full_content_word_count, share_urls, duration,
                 data_source):
        self.authors = authors
        self.canonical_url = canonical_url
        self.urls = urls
        self.page_type = page_type or 'no_metas'
        self.post_id = post_id
        self.pub_date_tmsp = pub_date_tmsp
        self.custom_metadata = custom_metadata
        self.section = section
        self.tags = tags
        self.save_date_tmsp = save_date_tmsp
        self.thumb_url = thumb_url
        self.title = title
        self.image_url = image_url
        self.full_content_word_count = full_content_word_count
        self.share_urls = share_urls
        self.duration = duration
        self.data_source = data_source


class SlotInfo(SlotsMixin):
    __slots__ = ('xpath', 'url', 'x', 'y')
    __version__ = 1

    def __init__(self, xpath, url, x, y):
        self.xpath = xpath
        self.url = url
        self.x = x
        self.y = y


class SessionInfo(SlotsMixin):
    __slots__ = ('id', 'timestamp', 'initial_url', 'initial_referrer',
                 'last_session_timestamp')
    __version__ = 1

    def __init__(self, id_, timestamp, initial_url, initial_referrer,
                 last_session_timestamp):
        self.id = id_
        self.timestamp = timestamp
        self.initial_url = initial_url
        self.initial_referrer = initial_referrer
        self.last_session_timestamp = last_session_timestamp


class TimestampInfo(SlotsMixin):
    """Holds timestamp info from an Event."""
    __slots__ = ('nginx_ms', 'pixel_ms', 'override_ms')
    __version__ = 1

    def __init__(self, nginx_ms, pixel_ms, override_ms):
        self.nginx_ms = nginx_ms
        self.pixel_ms = pixel_ms
        self.override_ms = override_ms


class VisitorInfo(SlotsMixin):
    """Holds visitor info sent in Event."""
    __slots__ = ('network_id', 'site_id', 'ip')
    __version__ = 1

    def __init__(self, network_id, site_id, ip):
        self.network_id = network_id
        self.site_id = site_id
        self.ip = ip


class DisplayInfo(SlotsMixin):
    """Holds info about displays from an Event."""
    __slots__ = ('total_width', 'total_height', 'avail_width', 'avail_height',
                 'pixel_depth')
    __version__ = 1

    def __init__(self, total_width, total_height, avail_width, avail_height,
                 pixel_depth):
        self.total_width = total_width
        self.total_height = total_height
        self.avail_width = avail_width
        self.avail_height = avail_height
        self.pixel_depth = pixel_depth


class CampaignInfo(SlotsMixin):
    """Holds info about campaign qsargs for an Event."""
    __slots__ = ('id', 'medium', 'source', 'content', 'term')
    __version__ = 1

    def __init__(self, id_, medium, source, content, term):
        self.id = id_
        self.medium = medium
        self.source = source
        self.content = content
        self.term = term


class Event(SlotsMixin):
    __slots__ = ('apikey', 'url', 'referrer', 'action', 'engaged_time_inc',
                 'visitor', 'extra_data', 'user_agent', 'display',
                 'timestamp_info', 'session', 'slot', 'metadata', 'campaign',
                 'flags')
    __version__ = 1

    def __init__(self, apikey, url, referrer, action, engaged_time_inc, visitor,
                 extra_data, user_agent, display, timestamp_info, session, slot,
                 metadata, campaign, flags):
        self.apikey = apikey
        self.url = url
        self.referrer = referrer
        self.action = action
        self.engaged_time_inc = engaged_time_inc
        self.visitor = visitor
        self.extra_data = extra_data
        self.user_agent = user_agent
        self.display = display
        self.timestamp_info = timestamp_info
        self.session = session
        self.slot = slot
        self.metadata = metadata
        self.campaign = campaign
        self.flags = flags

    def to_dict(self):
        """Return a Event represented as a dictionary."""
        event_dict = {
            'apikey': self.apikey,
            'url': self.url,
            'referrer': self.referrer,
            'action': self.action,
            'engaged_time_inc': self.engaged_time_inc,
            'extra_data': self.extra_data,
            'user_agent': self.user_agent,
        }
        # We store whether or not visitor, display, timestamp_info, and slot are
        # None in the dict to make sure we rebuild the same thing when
        # deserializing. Also saves a little space vs storing unnecessary keys.
        if self.visitor:
            event_dict['visitor'] = True
            event_dict['visitor.site_id'] = self.visitor.site_id
            event_dict['visitor.network_id'] = self.visitor.network_id
            event_dict['visitor.ip'] = self.visitor.ip
            event_dict['visitor.__version__'] = self.visitor.__version__
        else:
            event_dict['visitor'] = False
        if self.display:
            event_dict['display'] = True
            event_dict['display.total_width'] = self.display.total_width
            event_dict['display.total_height'] = self.display.total_height
            event_dict['display.avail_width'] = self.display.avail_width
            event_dict['display.avail_height'] = self.display.avail_height
            event_dict['display.pixel_depth'] = self.display.pixel_depth
            event_dict['display.__version__'] = self.display.__version__
        else:
            event_dict['display'] = False
        if self.timestamp_info:
            event_dict['timestamp_info'] = True
            event_dict['timestamp_info.nginx_ms'] = self.timestamp_info.nginx_ms
            event_dict['timestamp_info.pixel_ms'] = self.timestamp_info.pixel_ms
            event_dict['timestamp_info.override_ms'] = self.timestamp_info.override_ms
            event_dict['timestamp_info.__version__'] = self.timestamp_info.__version__
        else:
            event_dict['timestamp_info'] = False
        if self.session:
            event_dict['session'] = True
            event_dict['session.id'] = self.session.id
            event_dict['session.timestamp'] = self.session.timestamp
            event_dict['session.initial_url'] = self.session.initial_url
            event_dict['session.initial_referrer'] = self.session.initial_referrer
            event_dict['session.last_session_timestamp'] = self.session.last_session_timestamp
            event_dict['session.__version__'] = self.session.__version__
        else:
            event_dict['session'] = False
        if self.slot:
            event_dict['slot'] = True
            event_dict['slot.xpath'] = self.slot.xpath
            event_dict['slot.url'] = self.slot.url
            event_dict['slot.x'] = self.slot.x
            event_dict['slot.y'] = self.slot.y
            event_dict['slot.__version__'] = self.slot.__version__
        else:
            event_dict['slot'] = False
        if self.metadata:
            event_dict['metadata'] = True
            event_dict['metadata.authors'] = self.metadata.authors
            event_dict['metadata.canonical_url'] = self.metadata.canonical_url
            event_dict['metadata.urls'] = self.metadata.urls
            event_dict['metadata.page_type'] = self.metadata.page_type
            event_dict['metadata.post_id'] = self.metadata.post_id
            event_dict['metadata.pub_date_tmsp'] = self.metadata.pub_date_tmsp
            event_dict['metadata.custom_metadata'] = self.metadata.custom_metadata
            event_dict['metadata.section'] = self.metadata.section
            event_dict['metadata.tags'] = self.metadata.tags
            event_dict['metadata.save_date_tmsp'] = self.metadata.save_date_tmsp
            event_dict['metadata.thumb_url'] = self.metadata.thumb_url
            event_dict['metadata.title'] = self.metadata.title
            event_dict['metadata.image_url'] = self.metadata.image_url
            event_dict['metadata.full_content_word_count'] = self.metadata.full_content_word_count
            event_dict['metadata.share_urls'] = self.metadata.share_urls
            event_dict['metadata.duration'] = self.metadata.duration
            event_dict['metadata.data_source'] = self.metadata.data_source
            event_dict['metadata.__version__'] = self.metadata.__version__
        else:
            event_dict['metadata'] = False
        if self.campaign:
            event_dict['campaign'] = True
            event_dict['campaign.id'] = self.campaign.id
            event_dict['campaign.medium'] = self.campaign.medium
            event_dict['campaign.source'] = self.campaign.source
            event_dict['campaign.content'] = self.campaign.content
            event_dict['campaign.term'] = self.campaign.term
            event_dict['campaign.__version__'] = self.campaign.__version__
        else:
            event_dict['campaign'] = False
        if self.flags:
            event_dict['flags'] = True
            event_dict['flags.is_amp'] = self.flags.is_amp
            event_dict['flags.__version__'] = self.flags.__version__
        else:
            event_dict['flags'] = False
        event_dict['version'] = self.__version__
        return event_dict

    @classmethod
    def from_dict(cls, data):
        """Accepts a dictionary formatted as the output of to_dict, returns a new Event
        """
        # TODO: Add __version__ handling when necessary
        if data.get('display'):
            display = DisplayInfo(data.get('display.total_width'),
                                  data.get('display.total_height'),
                                  data.get('display.avail_width'),
                                  data.get('display.avail_height'),
                                  data.get('display.pixel_depth'))
        else:
            display = None
        if data.get('visitor'):
            visitor = VisitorInfo(data.get('visitor.network_id'),
                                  data.get('visitor.site_id'),
                                  data.get('visitor.ip'))
        else:
            visitor = None
        if data.get('timestamp_info'):
            timestamp_info = TimestampInfo(data.get('timestamp_info.nginx_ms'),
                                           data.get('timestamp_info.pixel_ms'),
                                           data.get('timestamp_info.override_ms'))
        else:
            timestamp_info = None
        if data.get('session'):
            session = SessionInfo(data.get('session.id'),
                                  data.get('session.timestamp'),
                                  data.get('session.initial_url'),
                                  data.get('session.initial_referrer'),
                                  data.get('session.last_session_timestamp'))
        else:
            session = None
        if data.get('slot'):
            slot = SlotInfo(data.get('slot.xpath'),
                            data.get('slot.url'),
                            data.get('slot.x'),
                            data.get('slot.y'))
        else:
            slot = None
        if data.get('metadata'):
            metadata = Metadata(data.get('metadata.authors'),
                                data.get('metadata.canonical_url'),
                                data.get('metadata.urls'),
                                data.get('metadata.page_type'),
                                data.get('metadata.post_id'),
                                data.get('metadata.pub_date_tmsp'),
                                data.get('metadata.custom_metadata'),
                                data.get('metadata.section'),
                                data.get('metadata.tags'),
                                data.get('metadata.save_date_tmsp'),
                                data.get('metadata.thumb_url'),
                                data.get('metadata.title'),
                                data.get('metadata.image_url'),
                                data.get('metadata.full_content_word_count'),
                                data.get('metadata.share_urls'),
                                data.get('metadata.duration'),
                                data.get('metadata.data_source'))
        else:
            metadata = None
        if data.get('campaign'):
            campaign = CampaignInfo(data.get('campaign.id'),
                                    data.get('campaign.medium'),
                                    data.get('campaign.source'),
                                    data.get('campaign.content'),
                                    data.get('campaign.term'))
        else:
            campaign = None
        if data.get('flags'):
            flags = EventFlags(data.get('flags.is_amp', False))
        else:
            flags = None
        return cls(data.get('apikey'),
                   data.get('url'),
                   data.get('referrer'),
                   data.get('action'),
                   data.get('engaged_time_inc'),
                   visitor,
                   data.get('extra_data'),
                   data.get('user_agent'),
                   display,
                   timestamp_info,
                   session,
                   slot,
                   metadata,
                   campaign,
                   flags)
