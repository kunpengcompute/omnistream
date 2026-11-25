/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include "PersonGenerator.h"
// The static field of PersonGenerator
const std::vector<std::string> PersonGenerator::US_STATES = {"AZ", "CA", "ID", "OR", "WA", "WY"};
const std::vector<std::string> PersonGenerator::US_CITIES = {
    "Phoenix", "Los Angeles", "San Francisco", "Boise", "Portland", "Bend", "Redmond", "Seattle", "Kent", "Cheyenne"};
const std::vector<std::string> PersonGenerator::FIRST_NAMES = {
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter"};
const std::vector<std::string> PersonGenerator::LAST_NAMES = {
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris"};
const std::vector<std::string> PersonGenerator::CREDIT_CARD_STRINGS = PersonGenerator::createCreditCardStrings();

long PersonGenerator::nextBase0PersonId(long eventId, SplittableRandom& random, const GeneratorConfig& config)
{
    long numPeople = lastBase0PersonId(config, eventId) + 1;
    long activePeople = std::min(numPeople, static_cast<long>(config.getNumActivePeople()));
    long n = random.nextLong(activePeople + PERSON_ID_LEAD);
    return numPeople - activePeople + n;
}

long PersonGenerator::lastBase0PersonId(const GeneratorConfig& config, long eventId)
{
    if (config.totalProportion == 0) {
        GErrorLog("event num is 0");
        throw std::runtime_error("event num is 0");
    }
    long epoch = eventId / config.totalProportion;
    long offset = eventId % config.totalProportion;
    if (offset >= config.personProportion) {
        // About to generate an auction or bid.
        // Go back to the last person generated in this epoch.
        offset = config.personProportion - 1;
    }
    // About to generate a person.
    return epoch * config.personProportion + offset;
}

std::vector<std::string> PersonGenerator::createCreditCardStrings()
{
    std::ostringstream oss;

    std::vector<std::string> creditCardStrings(10000);
    for (size_t i = 0; i < creditCardStrings.size(); ++i) {
        std::ostringstream oss;
        oss << std::setw(4) << std::setfill('0') << i;
        creditCardStrings[i] = oss.str();
    }
    return creditCardStrings;
}

std::string PersonGenerator::nextCreditCard()
{
    std::ostringstream sb;
    for (int i = 0; i < 4; i++) {
        if (i > 0) {
            sb << ' ';
        }
        sb << CREDIT_CARD_STRINGS[random.nextInt((int)CREDIT_CARD_STRINGS.size())];
    }
    return sb.str();
}

std::unique_ptr<Person> PersonGenerator::nextPerson(long nextEventId, long timestamp, const GeneratorConfig& config)
{
    long id = lastBase0PersonId(config, nextEventId) + GeneratorConfig::FIRST_PERSON_ID;
    std::string name = nextPersonName();
    std::string_view email = nextEmail();
    std::string creditCard = nextCreditCard();
    std::string_view city = nextUSCity();
    std::string_view state = nextUSState();
    int currentSize =
        8 + static_cast<int>(name.length() + email.length() + creditCard.length() + city.length() + state.length());
    std::string_view extra =
        StringsGenerator::nextExtra(random, currentSize, config.getAvgPersonByteSize(), extraBuffer);
    return std::make_unique<Person>(id, name, email, creditCard, city, state, timestamp, extra);
}
