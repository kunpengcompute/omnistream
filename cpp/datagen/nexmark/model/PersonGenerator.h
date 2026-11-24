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

#ifndef OMNISTREAM_PERSONGENERATOR_H
#define OMNISTREAM_PERSONGENERATOR_H

#include "EventGenerator.h"
// PersonGenerator class definition, corresponding to com.github.nexmark.flink.generator.model.PersonGenerator
class PersonGenerator {
public:
    PersonGenerator() : random(0)
    {
        emailBuffer = new char[17 + 1];
        extraBuffer = new char[1024];
    }
    ~PersonGenerator()
    {
        delete[] emailBuffer;
        delete[] extraBuffer;
    }
    /** Generate and return a random person with next available id. */
    std::unique_ptr<Person> nextPerson(long nextEventId, long timestamp, const GeneratorConfig& config);
    /** Return a random person id (base 0). */
    static long nextBase0PersonId(long eventId, SplittableRandom& random, const GeneratorConfig& config);
    /**
     * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
     * due to generate a person.
     */
    static long lastBase0PersonId(const GeneratorConfig& config, long eventId);

private:
    SplittableRandom random;
    /** Number of yet-to-be-created people and auction ids allowed. */
    static const int PERSON_ID_LEAD = 10;

    /**
     * Keep the number of states small so that the example queries will find results even with a small
     * batch of events.
     */
    static const std::vector<std::string> US_STATES;

    static const std::vector<std::string> US_CITIES;

    static const std::vector<std::string> FIRST_NAMES;

    static const std::vector<std::string> LAST_NAMES;

    static const std::vector<std::string> CREDIT_CARD_STRINGS; // initialized via createCreditCardStrings()

    /** Return a random credit card number. */
    inline std::string nextCreditCard();

    /** return a random US state. */
    inline std::string_view nextUSState()
    {
        // These are
        return US_STATES[random.nextInt((int)US_STATES.size())];
    }

    /** Return a random US city. */
    inline std::string_view nextUSCity()
    {
        return US_CITIES[random.nextInt((int)US_CITIES.size())];
    }

    /** Return a random person name. */
    inline std::string nextPersonName()
    {
        return FIRST_NAMES[random.nextInt((int)FIRST_NAMES.size())] + " " +
               LAST_NAMES[random.nextInt((int)LAST_NAMES.size())];
    }

    /** Return a random email address. */
    inline std::string_view nextEmail()
    {
        std::string email =
            StringsGenerator::nextString(random, 7) + "@" + StringsGenerator::nextString(random, 5) + ".com";
        email.copy(emailBuffer, email.size());
        return std::string_view(emailBuffer, email.size());
    }

private:
    static std::vector<std::string> createCreditCardStrings();
    char* emailBuffer;
    char* extraBuffer;
};

#endif // OMNISTREAM_PERSONGENERATOR_H
