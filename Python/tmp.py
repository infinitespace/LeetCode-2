# THRESHOLD between journey, if trip2.starttime - trip1.endtime < THRESHOLD, 
#   trip1 and trip2 are in the same journey
THRESHOLD = 1 day 

def find_journeys(trips):
    # Input: an iterator of trip records, not null
    # Output: an iterator of journey records
    journeys = []
    journeys.append({starttime: trips[0].starttime, endtime: trips[0].endtime})
    for trip in trips:
        if trip.starttime != journeys[-1].starttime:    # find an unclustered trip
            if trip.starttime - journeys[-1].endtime < THRESHOLD:
                journeys[-1].endtime = trip.endtime             # clustered to the last journey
            else:
                journeys.append({starttime: trip.starttime, endtime: trip.endtime}) # add a new journey
    return journeys

'''
# Prepocessing: Split data
1. Sort data by by alphabetically ordered traveller id.
2. Split data to sections and try to keep data with the same traveller id is in the same section.
   SectionLength is smaller than workers memory constrain and trys to be larger than the max data 
   size of one traveller id
'''
ONESECTION = sectionLength 

'''
If some traveller id have to many records that can not be fitted in to one section, thus have to
be store to different sections, create a List to store those sections' indices, which need to be 
combined later.

We want to keep the length of Combine_List to be short, and there is no over lapped intervals 
between each element in Combine_List.
'''
Combine_List = [[0,1],[23,24,25],[453,454,455]]
Sections = [section_0, section_1, ..., section_N]


# Assume there are N sections of data
for section in range(section_0, section_N, ONESECTION):
    trip = sc.textFile(prefixUri + "trips/" + section) 
    trip_labelled = trips.map(lambda t: (t.split(',')[0], t)) 
    user_trips = trip_labelled.groupByKey()
    user_journey = user_trips.flatMapValues(find_journeys) 
    journey = user_journey.map(lambda uj: uj[1]) 
    journey.saveAsTextFile(prefixUri + "journeys/" + section)

# For those sections that need to be combined, combine their journeys 
for journey_section_idx_list in Combine_List:
    journey_sections = load all journey sections in this journey_section_idx_list # e.g section [23,24,25]
    journey_labelled = journey_sections.map(lambda t: (t.split(',')[0], t))
    user_journeys = journey_labelled.groupByKey()
    user_journey = user_journeys.flatMapValues(find_journeys)
    journey = user_journey.map(lambda uj: uj[1]) 
    journey.saveAsTextFile(prefixUri + "final_journeys/" + str(journey_section_idx_list))



