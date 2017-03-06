from parsely_raw_data.event import (Event, VisitorInfo, TimestampInfo,
                                    DisplayInfo, SessionInfo, SlotInfo,
                                    Metadata, CampaignInfo, EventFlags)


event = Event(
    'example.com',
    'http://localhost:8000/examples.build/analytics.amp.html',
    None,
    'pageview',
    None,
    VisitorInfo(
        None,
        'amp-kiZ6-WZla1kXFFWAw2oxfLyJVWB8ytaeJ7ghXsXe5',
        '198.200.78.40'),
    None,
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36'
    ' (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36',
    DisplayInfo(1920, 1200, 1916, 1177, 24),
    TimestampInfo(1451685961000, 1455027478791, None),
    SessionInfo(
        12345,
        1457737463000,
        "http://test.com",
        "http://test2.com",
        1457737463000
    ),
    SlotInfo("/html", "http://test.com/nothing", 400, 400),
    Metadata(
        ["Walt Whitman"],
        "http://parsely.com/testpost",
        ["http://parsely.com/testpost"],
        "post",
        "abcdefg",
        1451685961000,
        None,
        "vertical",
        ["tag1", "tag2"],
        1451685961000,
        "http://parsely.com/thumburl",
        "This is the title of the thing",
        "http://parsely.com/imgurl",
        420,
        ["http://twitter.com/nothing"],
        69,
        'crawl'
    ),
    CampaignInfo('spring_sale', 'email', 'newsletter', 'logolink', 'foo'),
    EventFlags(False),
)

def test_to_dict_checker():
    """Make sure that the length of __slots__ hasn't changed.

    If __slots__ has changed, tell the user to make sure they update
    `.to_dict` in the offending class. Otherwise, we have no way of
    making sure the two stay in sync.
    """
    msg = "It looks like an object has changed. Please be sure to update to_dict before updating this test to pass."
    assert len(DisplayInfo.__slots__) == 5, msg
    assert len(Event.__slots__) == 15, msg
    assert len(SessionInfo.__slots__) == 5, msg
    assert len(SlotInfo.__slots__) == 4, msg
    assert len(TimestampInfo.__slots__) == 3, msg
    assert len(VisitorInfo.__slots__) == 3, msg
    assert len(Metadata.__slots__) == 17, msg
    assert len(CampaignInfo.__slots__) == 5, msg
    assert len(EventFlags.__slots__) == 1, msg


def test_dict():
    serded = Event.from_dict(event.to_dict())
    assert serded == event


def test_missing_extra():
    """Ensure we still have extra after to_dict and from_dict."""
    extra_data = {'important': 'No throwing away!'}
    event = Event(apikey='example.com',
                  url='http://www.example.com/',
                  referrer='http://www.example.com/article-123',
                  action='pageview',
                  engaged_time_inc=None,
                  extra_data=extra_data,
                  visitor=VisitorInfo(site_id='e71604df-a912-455d-aaf3-a9c72a6dd86c',
                                      network_id='',
                                      ip='184.149.39.120'),
                  user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
                  display=DisplayInfo(1440,
                                      900,
                                      1436,
                                      877,
                                      24),
                  timestamp_info=TimestampInfo(nginx_ms=1429707722000,
                                               pixel_ms=None,
                                               override_ms=None),
                  session=SessionInfo(5,
                                      1471428000000,
                                      'http://www.example.com/',
                                      'https://www.google.ca/',
                                      1470045600000),
                  slot=None,
                  metadata=None,
                  campaign=None,
                  flags=None)

    other = Event.from_dict(event.to_dict())
    assert other.extra_data == extra_data


if __name__ == "__main__":
    test_dict()
    test_to_dict_checker()
