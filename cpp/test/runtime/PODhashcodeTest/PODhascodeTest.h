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

#ifndef PODHASCODETEST_H
#define PODHASCODETEST_H
#include <stdexcept>
#include <utility>
#include <gtest/gtest.h>
#include "runtime/executiongraph/JobInformationPOD.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/executiongraph/descriptor/ExecutionAttemptIDPOD.h"
#include "runtime/shuffle/ShuffleIOOwnerContextPOD.h"
#include "runtime/executiongraph/common/IntermediateDataSetIDPOD.h"
#include "runtime/executiongraph/StreamPartitionerPOD.h"
#include "runtime/executiongraph/NonChainedOutputPOD.h"
#include "runtime/executiongraph/StreamEdgePOD.h"
#include "runtime/executiongraph/operatorchain/TypeDescriptionPOD.h"
#include "runtime/executiongraph/operatorchain/OperatorPOD.h"
#include "runtime/executiongraph/StreamConfigPOD.h"
#include "runtime/executiongraph/common/TaskPlainInfoPOD.h"
#include "runtime/executiongraph/descriptor/TaskDeploymentDescriptorPOD.h"
#include "runtime/executiongraph/descriptor/IntermediateResultPartitionIDPOD.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "runtime/executiongraph/descriptor/ResourceIDPOD.h"
#include "runtime/executiongraph/TaskInformationPOD.h"

#include "runtime/partition/ResultSubpartitionInfoPOD.h"

// for JobInformationPOD

TEST(PODhascodeTest, AbstractIDPOD)
{
    std::unordered_map<omnistream::AbstractIDPOD, int> maps;
    omnistream::AbstractIDPOD abs1(200000, 2000001);
    omnistream::AbstractIDPOD abs2(200000, 2000001);

    maps[abs1] = 1;
    // check put(key,old)
    EXPECT_EQ(maps[abs1], 1);

    // check put(key,new)  get(key)=new
    maps[abs1] = 2;
    EXPECT_EQ(maps[abs1], 2);

    // check remove key  get(key)==null
    maps.erase(abs1);
    EXPECT_EQ(maps[abs1], 0);

    // another key
    maps[abs2] = 3;
    maps[abs1] = 100;
    EXPECT_EQ(maps[abs2], 100);
    EXPECT_EQ(maps[abs1], 100);
}

TEST(PODhascodeTest, JobInformationPOD)
{
    // todo: create a JobInformationPOD instance
    std::unordered_map<omnistream::JobInformationPOD, int> maps;

    omnistream::JobIDPOD jobid1(20000, 20000);
    omnistream::JobIDPOD jobid2(20000, 20000);
    omnistream::JobIDPOD jobid3(20000, 20000);

    omnistream::JobInformationPOD jobinformation1(jobid1, "job1");
    omnistream::JobInformationPOD jobinformation2(jobid1, "job1");

    maps[jobinformation1] = 1;
    // check if map can find key
    EXPECT_EQ(maps[jobinformation1], 1);

    maps[jobinformation1] = 100;
    EXPECT_EQ(maps[jobinformation1], 100);

    maps.erase(jobinformation1);
    EXPECT_EQ(maps[jobinformation1], 0);

    maps[jobinformation2] = 11;
    EXPECT_EQ(maps[jobinformation1], 11);
    EXPECT_EQ(maps[jobinformation2], 11);
}

TEST(PODhascodeTest, JobInformationPODDefaultRecoverySavepointFormat)
{
    omnistream::JobIDPOD jobId(20000, 20000);
    omnistream::JobInformationPOD jobInformation(jobId, "job1");
    omnistream::JobInformationPOD jobInformationWithWatermark(jobId, "job2", 1000);

    EXPECT_EQ(
        jobInformation.getRecoverySavepointFormat(), omnistream::JobInformationPOD::RECOVERY_FORMAT_OMNI_INTERNAL);
    EXPECT_EQ(
        jobInformationWithWatermark.getRecoverySavepointFormat(),
        omnistream::JobInformationPOD::RECOVERY_FORMAT_OMNI_INTERNAL);
    EXPECT_FALSE(jobInformation.isCompatibleRecoverySavepointFormat());
}

TEST(PODhascodeTest, JobInformationPODRecoverySavepointFormat)
{
    omnistream::JobInformationPOD jobInformation(omnistream::JobIDPOD(20000, 20000), "job1");

    jobInformation.setRecoverySavepointFormat(omnistream::JobInformationPOD::RECOVERY_FORMAT_FLINK_COMPATIBLE);

    EXPECT_EQ(
        jobInformation.getRecoverySavepointFormat(), omnistream::JobInformationPOD::RECOVERY_FORMAT_FLINK_COMPATIBLE);
    EXPECT_TRUE(jobInformation.isCompatibleRecoverySavepointFormat());
    EXPECT_NO_THROW(jobInformation.checkRecoverySavepointFormat());

    jobInformation.setRecoverySavepointFormat(omnistream::JobInformationPOD::RECOVERY_FORMAT_OMNI_INTERNAL);
    EXPECT_FALSE(jobInformation.isCompatibleRecoverySavepointFormat());
    EXPECT_NO_THROW(jobInformation.checkRecoverySavepointFormat());

    jobInformation.setRecoverySavepointFormat("UNKNOWN");
    EXPECT_THROW(jobInformation.checkRecoverySavepointFormat(), std::runtime_error);
}

TEST(PODhascodeTest, JobInformationPODRecoverySavepointFormatCopyAndMove)
{
    omnistream::JobInformationPOD source(omnistream::JobIDPOD(20000, 20000), "job1", 1000);
    source.setRecoverySavepointFormat(omnistream::JobInformationPOD::RECOVERY_FORMAT_FLINK_COMPATIBLE);

    omnistream::JobInformationPOD copyConstructed(source);
    EXPECT_EQ(copyConstructed.getRecoverySavepointFormat(), source.getRecoverySavepointFormat());

    omnistream::JobInformationPOD copyAssigned;
    copyAssigned = source;
    EXPECT_EQ(copyAssigned.getRecoverySavepointFormat(), source.getRecoverySavepointFormat());

    omnistream::JobInformationPOD moveConstructed(std::move(copyConstructed));
    EXPECT_EQ(
        moveConstructed.getRecoverySavepointFormat(), omnistream::JobInformationPOD::RECOVERY_FORMAT_FLINK_COMPATIBLE);

    omnistream::JobInformationPOD moveAssigned;
    moveAssigned = std::move(copyAssigned);
    EXPECT_EQ(
        moveAssigned.getRecoverySavepointFormat(), omnistream::JobInformationPOD::RECOVERY_FORMAT_FLINK_COMPATIBLE);
}

TEST(PODhascodeTest, JobInformationPODRecoverySavepointFormatJsonSerialization)
{
    omnistream::JobInformationPOD jobInformation(omnistream::JobIDPOD(20000, 20000), "job1", 1000);
    jobInformation.setRecoverySavepointFormat(omnistream::JobInformationPOD::RECOVERY_FORMAT_FLINK_COMPATIBLE);

    nlohmann::json serialized = jobInformation;
    auto deserialized = serialized.get<omnistream::JobInformationPOD>();

    EXPECT_EQ(
        serialized["recoverySavepointFormat"].get<std::string>(),
        omnistream::JobInformationPOD::RECOVERY_FORMAT_FLINK_COMPATIBLE);
    EXPECT_EQ(deserialized.getJobId(), jobInformation.getJobId());
    EXPECT_EQ(deserialized.getJobName(), jobInformation.getJobName());
    EXPECT_EQ(deserialized.getAutoWatermarkInterval(), jobInformation.getAutoWatermarkInterval());
    EXPECT_EQ(deserialized.getRecoverySavepointFormat(), jobInformation.getRecoverySavepointFormat());
}

TEST(PODhascodeTest, JobInformationPODRecoverySavepointFormatToString)
{
    omnistream::JobInformationPOD jobInformation(omnistream::JobIDPOD(20000, 20000), "job1", 1000);

    EXPECT_NE(jobInformation.toString().find("recoverySavepointFormat='OMNI_INTERNAL'"), std::string::npos);
}

// for JobIDPOD
TEST(PODhascodeTest, JobIDPOD)
{
    // todo: create a JobInformationPOD instance
    std::unordered_map<omnistream::JobIDPOD, int> maps;
    omnistream::JobIDPOD jobid1(20000, 20000);
    omnistream::JobIDPOD jobid2(20000, 20000);

    maps[jobid1] = 1;

    // check can find the key
    EXPECT_EQ(maps[jobid1], 1);

    maps[jobid1] = 2;
    EXPECT_EQ(maps[jobid1], 2);

    maps.erase(jobid1);
    EXPECT_EQ(maps[jobid1], 0);

    maps[jobid2] = 2;
    EXPECT_EQ(maps[jobid1], 2);
    EXPECT_EQ(maps[jobid2], 2);
}

// for JobIDPOD
TEST(PODhascodeTest, ExecutionAttemptIDPOD)
{
    // todo: create a JobInformationPOD instance
    std::unordered_map<omnistream::ExecutionAttemptIDPOD, int> maps;

    auto execution1 = omnistream::ExecutionAttemptIDPOD::randomId();
    auto execution2 = execution1;

    EXPECT_EQ(maps[execution1], 0);

    // check1
    maps[execution1] = 1;
    EXPECT_EQ(maps[execution1], 1);

    // check2
    maps[execution2] = 100;
    EXPECT_EQ(maps[execution1], 100);

    // check3
    maps.erase(execution1);
    EXPECT_EQ(maps[execution1], 0);

    // check4
    maps[execution2] = 11;
    EXPECT_EQ(maps[execution1], 11);
    EXPECT_EQ(maps[execution2], 11);
}

TEST(PODhascodeTest, ShuffleIOOwnerContextPOD)
{
    std::unordered_map<omnistream::ShuffleIOOwnerContextPOD, int> maps;

    auto execution = omnistream::ExecutionAttemptIDPOD::randomId();

    omnistream::ShuffleIOOwnerContextPOD shuffle1("s1", execution);
    omnistream::ShuffleIOOwnerContextPOD shuffle2 = shuffle1;

    EXPECT_EQ(maps[shuffle1], 0);

    maps[shuffle1] = 1;
    EXPECT_EQ(maps[shuffle1], 1);

    maps[shuffle1] = 2;
    EXPECT_EQ(maps[shuffle1], 2);

    maps.erase(shuffle1);
    EXPECT_EQ(maps[shuffle1], 0);

    maps[shuffle2] = 10;
    EXPECT_EQ(maps[shuffle1], 10);
    EXPECT_EQ(maps[shuffle2], 10);
}

TEST(PODhascodeTest, IntermediateDataSetIDPOD)
{
    std::unordered_map<omnistream::IntermediateDataSetIDPOD, int> maps;
    omnistream::IntermediateDataSetIDPOD abs1(20000, 20000);
    omnistream::IntermediateDataSetIDPOD abs2(20000, 20000);
    omnistream::IntermediateDataSetIDPOD abs3(20000, 20000);

    maps[abs1] = 1;
    EXPECT_EQ(maps[abs1], 1);

    maps[abs1] = 2;
    EXPECT_EQ(maps[abs1], 2);

    maps.erase(abs1);
    EXPECT_EQ(maps[abs1], 0);

    maps[abs2] = 10;
    EXPECT_EQ(maps[abs1], 10);
    EXPECT_EQ(maps[abs2], 10);
}

TEST(PODhascodeTest, StreamPartitionerPOD)
{
    std::unordered_map<omnistream::StreamPartitionerPOD, int> maps;
    omnistream::KeyFieldInfoPOD k1("n1", "n2", 20000);
    omnistream::KeyFieldInfoPOD k2("n1", "n2", 20000);
    omnistream::KeyFieldInfoPOD k3("n1", "n2", 20000);
    // omnistream::KeyFieldInfoPOD k4("n4","n2",100);
    // omnistream::KeyFieldInfoPOD k5("n5","n2",200);
    // omnistream::KeyFieldInfoPOD k6("n6","n2",3001);

    std::vector<omnistream::KeyFieldInfoPOD> hashFields1 = {k1, k2, k3};
    // std::vector<omnistream::KeyFieldInfoPOD> hashFields2={k4,k5,k6};

    omnistream::StreamPartitionerPOD s1("s1", hashFields1);
    omnistream::StreamPartitionerPOD s2("s1", hashFields1);

    maps[s1] = 1;
    EXPECT_EQ(maps[s1], 1);

    maps[s1] = 2;
    EXPECT_EQ(maps[s1], 2);

    maps.erase(s1);
    EXPECT_EQ(maps[s1], 0);
    maps[s2] = 10;
    EXPECT_EQ(maps[s2], 10);
    EXPECT_EQ(maps[s1], 10);
}

TEST(PODhascodeTest, KeyFieldInfoPOD)
{
    std::unordered_map<omnistream::KeyFieldInfoPOD, int> maps;
    omnistream::KeyFieldInfoPOD k1("n1", "n2", 20000);
    omnistream::KeyFieldInfoPOD k2("n1", "n2", 20000);

    maps[k1] = 1;
    EXPECT_EQ(maps[k1], 1);

    maps[k1] = 2;
    EXPECT_EQ(maps[k1], 2);

    maps.erase(k1);
    EXPECT_EQ(maps[k1], 0);

    maps[k2] = 200;
    EXPECT_EQ(maps[k1], 200);
    EXPECT_EQ(maps[k2], 200);
}

TEST(PODhascodeTest, NonChainedOutputPOD)
{
    std::unordered_map<omnistream::NonChainedOutputPOD, int> maps;
    omnistream::IntermediateDataSetIDPOD abs1(20000, 20000);
    omnistream::IntermediateDataSetIDPOD abs2(20001, 20000);
    omnistream::IntermediateDataSetIDPOD abs3(20000, 20001);

    omnistream::KeyFieldInfoPOD k1("n1", "n2", 200);
    omnistream::KeyFieldInfoPOD k2("n2", "n2", 20000);
    omnistream::KeyFieldInfoPOD k3("n3", "n2", 20001);
    omnistream::KeyFieldInfoPOD k4("n4", "n2", 100);
    omnistream::KeyFieldInfoPOD k5("n5", "n2", 200);
    omnistream::KeyFieldInfoPOD k6("n6", "n2", 3001);
    std::vector<omnistream::KeyFieldInfoPOD> hashFields1 = {k1, k2, k3};
    std::vector<omnistream::KeyFieldInfoPOD> hashFields2 = {k4, k5, k6};

    omnistream::StreamPartitionerPOD s1("s1", hashFields1);
    omnistream::StreamPartitionerPOD s2("s2", hashFields2);

    omnistream::NonChainedOutputPOD nc1(true, 30, 40, 50, 2000000, abs1, false, s1, 1);
    omnistream::NonChainedOutputPOD nc2(true, 30, 40, 50, 2000000, abs1, false, s1, 1);

    maps[nc1] = 1;
    EXPECT_EQ(maps[nc1], 1);
    maps[nc1] = 2;
    EXPECT_EQ(maps[nc1], 2);
    maps.erase(nc1);
    EXPECT_EQ(maps[nc1], 0);

    maps[nc2] = 200;
    EXPECT_EQ(maps[nc1], 200);
    EXPECT_EQ(maps[nc2], 200);
}

TEST(PODhascodeTest, StreamEdgePOD)
{
    std::unordered_map<omnistream::StreamEdgePOD, int> maps;

    omnistream::KeyFieldInfoPOD k1("n1", "n2", 200);
    omnistream::KeyFieldInfoPOD k2("n2", "n2", 20000);
    omnistream::KeyFieldInfoPOD k3("n3", "n2", 20001);
    omnistream::KeyFieldInfoPOD k4("n4", "n2", 100);
    omnistream::KeyFieldInfoPOD k5("n5", "n2", 200);
    omnistream::KeyFieldInfoPOD k6("n6", "n2", 3001);
    std::vector<omnistream::KeyFieldInfoPOD> hashFields1 = {k1, k2, k3};
    std::vector<omnistream::KeyFieldInfoPOD> hashFields2 = {k4, k5, k6};

    omnistream::StreamPartitionerPOD s1("s1", hashFields1);
    omnistream::StreamPartitionerPOD s2("s2", hashFields2);
    omnistream::StreamEdgePOD sp1(1, 2, 3, s1, false);
    omnistream::StreamEdgePOD sp2(1, 2, 3, s1, false);

    maps[sp1] = 1;
    EXPECT_EQ(maps[sp1], 1);

    maps[sp1] = 3;
    EXPECT_EQ(maps[sp1], 3);
    maps.erase(sp1);
    EXPECT_EQ(maps[sp1], 0);

    maps[sp2] = 2;
    EXPECT_EQ(maps[sp1], 2);
    EXPECT_EQ(maps[sp2], 2);
}

TEST(PODhascodeTest, TypeDescriptionPOD)
{
    std::unordered_map<omnistream::TypeDescriptionPOD, int> maps;
    omnistream::TypeDescriptionPOD k1("a", false, 30, "b", 30, "po");
    omnistream::TypeDescriptionPOD k2("a", false, 30, "b", 30, "po");

    maps[k1] = 1;
    EXPECT_EQ(maps[k1], 1);

    maps[k1] = 2;
    EXPECT_EQ(maps[k1], 2);
    maps.erase(k1);
    EXPECT_EQ(maps[k1], 0);
    maps[k2] = 10;
    EXPECT_EQ(maps[k2], 10);
    EXPECT_EQ(maps[k1], 10);
}

TEST(PODhascodeTest, OperatorPOD)
{
    std::unordered_map<omnistream::OperatorPOD, int> maps;

    omnistream::TypeDescriptionPOD k1("a", false, 30, "b", 30, "po");
    omnistream::TypeDescriptionPOD k2("b", true, 40, "a", 50, "pod");
    omnistream::TypeDescriptionPOD k3("c", false, 50, "c", 30, "po");
    omnistream::TypeDescriptionPOD k4("d", true, 60, "d", 40, "podman");

    std::vector<omnistream::TypeDescriptionPOD> hashFields1 = {k1, k2, k3};
    std::vector<omnistream::TypeDescriptionPOD> hashFields2 = {k2, k3, k4};

    omnistream::OperatorPOD op1("zirui", "1", "engineer", hashFields1, k4);
    omnistream::OperatorPOD op2("zirui", "1", "engineer", hashFields1, k4);

    maps[op1] = 1;
    EXPECT_EQ(maps[op1], 1);

    maps[op1] = 2;
    EXPECT_EQ(maps[op1], 2);

    maps.erase(op1);
    EXPECT_EQ(maps[op1], 0);
    maps[op2] = 10;
    EXPECT_EQ(maps[op2], 10);
    EXPECT_EQ(maps[op1], 10);
}

TEST(PODhascodeTest, StreamConfigPOD)
{
    std::unordered_map<omnistream::StreamConfigPOD, int> maps;
    //
    omnistream::KeyFieldInfoPOD k1("n1", "n2", 200);
    omnistream::KeyFieldInfoPOD k2("n2", "n2", 20000);
    omnistream::KeyFieldInfoPOD k3("n3", "n2", 20001);
    omnistream::KeyFieldInfoPOD k4("n4", "n2", 100);
    omnistream::KeyFieldInfoPOD k5("n5", "n2", 200);
    omnistream::KeyFieldInfoPOD k6("n6", "n2", 3001);
    std::vector<omnistream::KeyFieldInfoPOD> hashFields1 = {k1, k2, k3};
    std::vector<omnistream::KeyFieldInfoPOD> hashFields2 = {k4, k5, k6};
    //
    //
    omnistream::StreamPartitionerPOD s1("s1", hashFields1);
    omnistream::StreamPartitionerPOD s2("s2", hashFields2);
    omnistream::StreamEdgePOD sp1(1, 2, 3, s1, false);
    omnistream::StreamEdgePOD sp2(1, 2, 4, s2, true);
    omnistream::StreamEdgePOD sp3(1, 20, 3, s1, true);
    omnistream::StreamEdgePOD sp4(1, 12, 4, s2, true);
    omnistream::StreamEdgePOD sp5(1, 20, 3, s1, false);
    omnistream::StreamEdgePOD sp6(1, 12, 14, s2, true);
    //
    const std::vector<omnistream::StreamEdgePOD>& o = {sp1, sp2, sp3};  // todo
    const std::vector<omnistream::StreamEdgePOD>& o2 = {sp4, sp5, sp6}; // todo
    //
    //
    omnistream::TypeDescriptionPOD t1("a", false, 30, "b", 30, "po");
    omnistream::TypeDescriptionPOD t2("b", true, 40, "a", 50, "pod");
    omnistream::TypeDescriptionPOD t3("c", false, 50, "c", 30, "po");
    const omnistream::TypeDescriptionPOD t4("d", true, 60, "d", 40, "podman");
    const std::vector<omnistream::TypeDescriptionPOD> hashFields3 = {t1, t2, t3};
    const std::vector<omnistream::TypeDescriptionPOD> hashFields4 = {t2, t3, t4};
    omnistream::OperatorPOD op1("zirui", "1", "engineer", hashFields3, t4); // todo
    //
    //    OperatorPOD(const std::string& name, const std::string& id, const std::string& description,
    // const std::vector<TypeDescriptionPOD> &inputs,
    // const TypeDescriptionPOD &output

    omnistream::IntermediateDataSetIDPOD abs1(20000, 20000);
    omnistream::IntermediateDataSetIDPOD abs2(20001, 20000);
    omnistream::IntermediateDataSetIDPOD abs3(20000, 20001);
    //
    //
    omnistream::NonChainedOutputPOD nc1(true, 30, 40, 50, 2000000, abs1, false, s1, 1);
    omnistream::NonChainedOutputPOD nc2(true, 40, 50, 60, 5000000, abs2, true, s1, 1);
    omnistream::NonChainedOutputPOD nc3(true, 30, 50, 50, 2000000, abs1, false, s1, 1);
    //
    std::vector<omnistream::NonChainedOutputPOD> opNonChainedOutputs = {nc1, nc2, nc3}; // todo
    //
    std::vector<omnistream::StreamEdgePOD> inStreamEdges;
    std::unordered_map<std::string, std::string> omniConf;
    omnistream::StreamConfigPOD spod(o, "backend", opNonChainedOutputs, o2, op1, "zack", inStreamEdges, 0, omniConf);
    omnistream::StreamConfigPOD spod2(o, "backend", opNonChainedOutputs, o2, op1, "zack", inStreamEdges, 0, omniConf);
    // std::cout << "Hash value for spod: " << std::hash<omnistream::StreamConfigPOD>{}(spod) << std::endl;
    // std::cout << "Hash value for spod2: " << std::hash<omnistream::StreamConfigPOD>{}(spod2) << std::endl;
    // std::cout << "Are spod and spod2 equal? " << (spod == spod2 ? "Yes" : "No") << std::endl;
    //
    //
    // std::cout << "Inserting spod..." << std::endl;
    // std::cout << "Hash value for spod: " << std::hash<omnistream::StreamConfigPOD>{}(spod) << std::endl;
    // maps[spod] = 1;
    // std::cout << "maps[spod]: " << maps[spod] << std::endl;
    //
    // std::cout << "Inserting spod2..." << std::endl;
    // std::cout << "Hash value for spod2: " << std::hash<omnistream::StreamConfigPOD>{}(spod2) << std::endl;
    // maps[spod2] = 1;
    // std::cout << "maps[spod2]: " << maps[spod2] << std::endl;
    //
    // //
    maps[spod] = 1;
    EXPECT_EQ(maps[spod], 1);

    maps[spod] = 2;
    EXPECT_EQ(maps[spod], 2);

    maps.erase(spod);
    EXPECT_EQ(maps[spod], 0);

    maps[spod2] = 10;
    EXPECT_EQ(maps[spod], 10);
    EXPECT_EQ(maps[spod2], 10);

    std::vector<omnistream::StreamConfigPOD> v1 = {spod, spod2};

    omnistream::TypeDescriptionPOD g1("a", false, 30, "b", 30, "po");
    omnistream::TypeDescriptionPOD g2("b", true, 40, "a", 50, "pod");
    omnistream::TypeDescriptionPOD g3("c", false, 50, "c", 30, "po");
    omnistream::TypeDescriptionPOD g4("d", true, 60, "d", 40, "podman");

    std::vector<omnistream::TypeDescriptionPOD> hashFields5 = {g1, g2, g3};

    omnistream::OperatorPOD op11("zirui", "1", "engineer", hashFields5, g4);
    omnistream::OperatorPOD op12("zirui", "1", "engineer", hashFields5, g4);
    std::vector<omnistream::OperatorPOD> v2 = {op11, op12};
    omnistream::OperatorChainPOD oppp(v2);

    std::unordered_map<omnistream::TaskInformationPOD, int> maps2;

    omnistream::TaskInformationPOD Tpod("zack", 10, 20, 30, spod, oppp, v1);
    omnistream::TaskInformationPOD Tpod2("zack", 10, 20, 30, spod, oppp, v1);

    maps2[Tpod] = 1;
    EXPECT_EQ(maps2[Tpod], 1);
    maps2[Tpod] = 2;
    EXPECT_EQ(maps2[Tpod], 2);
    maps2.erase(Tpod);
    EXPECT_EQ(maps2[Tpod], 0);

    maps2[Tpod2] = 10;
    EXPECT_EQ(maps2[Tpod2], 10);
    EXPECT_EQ(maps2[Tpod], 10);

    //
}
//

TEST(PODhascodeTest, TaskPlainInfoPOD)
{
    std::unordered_map<omnistream::TaskPlainInfoPOD, int> maps;

    omnistream::TaskPlainInfoPOD taskplain("a1", "b1", "c1", 1, 2, 3, 4);
    omnistream::TaskPlainInfoPOD taskplain2("a1", "b1", "c1", 1, 2, 3, 4);

    maps[taskplain] = 1;
    EXPECT_EQ(maps[taskplain], 1);

    maps[taskplain] = 2;
    EXPECT_EQ(maps[taskplain], 2);

    maps.erase(taskplain);
    EXPECT_EQ(maps[taskplain], 0);

    maps[taskplain2] = 10;
    EXPECT_EQ(maps[taskplain], 10);
    EXPECT_EQ(maps[taskplain2], 10);
}

TEST(PODhascodeTest, IntermediateResultPartitionIDPOD)
{
    std::unordered_map<omnistream::IntermediateResultPartitionIDPOD, int> maps;
    omnistream::AbstractIDPOD abs1(20000, 20000);
    omnistream::AbstractIDPOD abs2(20001, 20000);
    omnistream::AbstractIDPOD abs3(20000, 20001);
    omnistream::IntermediateResultPartitionIDPOD Irp(abs1, 15);
    omnistream::IntermediateResultPartitionIDPOD Irp2(abs1, 15);

    maps[Irp] = 1;
    EXPECT_EQ(maps[Irp], 1);

    maps[Irp] = 2;
    EXPECT_EQ(maps[Irp], 2);

    maps.erase(Irp);
    EXPECT_EQ(maps[Irp], 0);

    maps[Irp2] = 10;
    EXPECT_EQ(maps[Irp], 10);
    EXPECT_EQ(maps[Irp2], 10);
}

TEST(PODhascodeTest, PartitionInfoPOD)
{
    // std::unordered_map<omnistream::PartitionInfoPOD,int>maps;
    // omnistream::AbstractIDPOD abs1(20000,20000);
    // omnistream::AbstractIDPOD abs2(20001,20000);
    // omnistream::AbstractIDPOD abs3(20000,20001);
    // omnistream::IntermediateResultPartitionIDPOD Irp(abs1,15);
    // maps[Irp]=1;
    // EXPECT_EQ(maps[Irp], 1);
}

TEST(PODhascodeTest, ResultPartitionIDPOD)
{
    std::unordered_map<omnistream::ResultPartitionIDPOD, int> maps;
    omnistream::AbstractIDPOD abs1(20000, 20000);
    omnistream::IntermediateResultPartitionIDPOD Ipid(abs1, 10);
    auto execution = omnistream::ExecutionAttemptIDPOD::randomId();
    omnistream::ResultPartitionIDPOD rPod(Ipid, execution);
    omnistream::ResultPartitionIDPOD rPod2 = rPod;

    EXPECT_EQ(maps[rPod], 0);

    maps[rPod] = 1;
    EXPECT_EQ(maps[rPod], 1);

    maps[rPod] = 2;
    EXPECT_EQ(maps[rPod], 2);
    maps.erase(rPod);
    EXPECT_EQ(maps[rPod], 0);
    maps[rPod2] = 10;
    EXPECT_EQ(maps[rPod2], 10);
    EXPECT_EQ(maps[rPod], 10);
}

TEST(PODhascodeTest, ResourceIDPOD)
{
    std::unordered_map<omnistream::ResourceIDPOD, int> maps;
    omnistream::ResourceIDPOD reID("zirui", "Canada Vancouver");
    omnistream::ResourceIDPOD reID2("zirui", "Canada Vancouver");

    maps[reID] = 1;
    EXPECT_EQ(maps[reID], 1);

    maps[reID] = 2;
    EXPECT_EQ(maps[reID], 2);
    maps.erase(reID);
    EXPECT_EQ(maps[reID], 0);

    maps[reID2] = 10;
    EXPECT_EQ(maps[reID], 10);

    EXPECT_EQ(maps[reID2], 10);
}

TEST(PODhascodeTest, ShuffleDescriptorPOD)
{
    // ResultPartitionIDPOD resultPartitionID; // Class: ResultPartitionIDPOD, Variable: resultPartitionID
    // bool unknown;                     // Class: bool, Variable: isUnknown
    // bool hasLocalResource;               // Class: bool, Variable: hasLocalResource
    // ResourceIDPOD storesLocalResourcesOn;   // Class: ResourceIDPOD, Variable: storesLocalResourcesOn

    std::unordered_map<omnistream::ShuffleDescriptorPOD, int> maps;
    omnistream::ResourceIDPOD reID("zirui", "Canada Vancouver");

    omnistream::AbstractIDPOD abs1(20000, 20000);
    omnistream::IntermediateResultPartitionIDPOD Ipid(abs1, 10);

    auto execution = omnistream::ExecutionAttemptIDPOD::randomId();
    omnistream::ResultPartitionIDPOD rPod(Ipid, execution);

    omnistream::ShuffleDescriptorPOD Slpd(rPod, false, true, reID);
    omnistream::ShuffleDescriptorPOD Slpd2 = Slpd;

    maps[Slpd] = 1;
    EXPECT_EQ(maps[Slpd], 1);

    maps[Slpd] = 2;
    EXPECT_EQ(maps[Slpd], 2);

    maps.erase(Slpd);
    EXPECT_EQ(maps[Slpd], 0);

    maps[Slpd2] = 10;
    EXPECT_EQ(maps[Slpd2], 10);

    EXPECT_EQ(maps[Slpd], 10);
}

TEST(PODhascodeTest, InputGateDeploymentDescriptorPOD)
{
    std::unordered_map<omnistream::InputGateDeploymentDescriptorPOD, int> maps;

    omnistream::IntermediateDataSetIDPOD ibs1(20000, 20000);
    omnistream::IntermediateDataSetIDPOD ibs2(20001, 20000);
    omnistream::IntermediateDataSetIDPOD ibs3(20000, 20001);

    omnistream::AbstractIDPOD abs1(20000, 20000);
    omnistream::AbstractIDPOD abs2(20001, 10000);

    omnistream::IntermediateResultPartitionIDPOD Ipid1(abs1, 10);
    omnistream::IntermediateResultPartitionIDPOD Ipid2(abs2, 110);

    auto execution = omnistream::ExecutionAttemptIDPOD::randomId();
    auto execution2 = omnistream::ExecutionAttemptIDPOD::randomId();

    omnistream::ResultPartitionIDPOD rPod1(Ipid1, execution);
    omnistream::ResultPartitionIDPOD rPod2(Ipid2, execution2);

    omnistream::ResourceIDPOD reID("zirui", "Canada Vancouver");
    omnistream::ResourceIDPOD reID2("leo", "Canada TOronto");

    omnistream::ResourceIDPOD reID3("zirui1", "Canada Vancouver1");
    omnistream::ResourceIDPOD reID4("leo1", "Canada TOronto2");

    omnistream::ShuffleDescriptorPOD Slpd(rPod1, false, true, reID);
    omnistream::ShuffleDescriptorPOD Slpd2(rPod1, false, true, reID2);
    omnistream::ShuffleDescriptorPOD Slpd3(rPod2, false, true, reID3);
    omnistream::ShuffleDescriptorPOD Slpd4(rPod2, false, true, reID4);

    std::vector<omnistream::ShuffleDescriptorPOD> shuffle = {Slpd, Slpd2};
    std::vector<omnistream::ShuffleDescriptorPOD> shuffle2 = {Slpd3, Slpd4};

    omnistream::InputGateDeploymentDescriptorPOD IDpod(ibs1, 10, 10, shuffle);
    omnistream::InputGateDeploymentDescriptorPOD IDpod2(ibs1, 10, 10, shuffle);

    maps[IDpod] = 1;
    EXPECT_EQ(maps[IDpod], 1);
    maps[IDpod] = 2;
    EXPECT_EQ(maps[IDpod], 2);

    maps.erase(IDpod);
    EXPECT_EQ(maps[IDpod], 0);

    maps[IDpod2] = 10;
    EXPECT_EQ(maps[IDpod], 10);
    EXPECT_EQ(maps[IDpod2], 10);
}

TEST(PODhascodeTest, ResultPartitionDeploymentDescriptorPOD)
{
    std::unordered_map<omnistream::ResultPartitionDeploymentDescriptorPOD, int> maps;

    omnistream::ResourceIDPOD reID("zirui", "Canada Vancouver");
    omnistream::ResourceIDPOD reID2("leo", "Canada TOronto");

    omnistream::ResourceIDPOD reID3("zirui1", "Canada Vancouver1");
    omnistream::ResourceIDPOD reID4("leo1", "Canada TOronto2");

    omnistream::AbstractIDPOD abs1(20000, 20000); // TODO
    omnistream::AbstractIDPOD abs2(20001, 10000);

    omnistream::IntermediateResultPartitionIDPOD Ipid1(abs1, 10); // TODO
    omnistream::IntermediateResultPartitionIDPOD Ipid2(abs2, 110);

    auto execution1 = omnistream::ExecutionAttemptIDPOD::randomId();
    auto execution2 = omnistream::ExecutionAttemptIDPOD::randomId();

    omnistream::ResultPartitionIDPOD rPod1(Ipid1, execution1);
    omnistream::ResultPartitionIDPOD rPod2(Ipid2, execution2);

    omnistream::ShuffleDescriptorPOD shuffleDescriptor(rPod1, false, true, reID); // TODO
    omnistream::ShuffleDescriptorPOD Slpd2(rPod1, false, true, reID2);
    omnistream::ShuffleDescriptorPOD Slpd3(rPod2, false, true, reID3);
    omnistream::ShuffleDescriptorPOD Slpd4(rPod2, false, true, reID4);

    omnistream::ResultPartitionDeploymentDescriptorPOD result_partition_deployment_descriptor_pod(
        shuffleDescriptor, 10, false, abs1, 13, Ipid1, 15, 16);

    omnistream::ResultPartitionDeploymentDescriptorPOD result_partition_deployment_descriptor_pod2(
        shuffleDescriptor, 10, false, abs1, 13, Ipid1, 15, 16);

    maps[result_partition_deployment_descriptor_pod] = 1;
    EXPECT_EQ(maps[result_partition_deployment_descriptor_pod], 1);

    maps[result_partition_deployment_descriptor_pod] = 2;
    EXPECT_EQ(maps[result_partition_deployment_descriptor_pod], 2);

    maps.erase(result_partition_deployment_descriptor_pod);
    EXPECT_EQ(maps[result_partition_deployment_descriptor_pod], 0);

    maps[result_partition_deployment_descriptor_pod2] = 10;
    EXPECT_EQ(maps[result_partition_deployment_descriptor_pod2], 10);
    EXPECT_EQ(maps[result_partition_deployment_descriptor_pod], 10);
}

TEST(PODhascodeTest, TaskDeploymentDescriptorPOD)
{
    std::unordered_map<omnistream::TaskDeploymentDescriptorPOD, int> maps;

    // JobIDPOD jobId;
    // std::vector<ResultPartitionDeploymentDescriptorPOD> producedPartitions;
    // std::vector<InputGateDeploymentDescriptorPOD> inputGates;

    omnistream::JobIDPOD jobid1(20000, 20000);
    omnistream::JobIDPOD jobid2(20000, 20000);
    omnistream::JobIDPOD jobid3(20000, 20000);

    omnistream::ResourceIDPOD reID("zirui", "Canada Vancouver");
    omnistream::ResourceIDPOD reID2("leo", "Canada TOronto");

    omnistream::ResourceIDPOD reID3("zirui1", "Canada Vancouver1");
    omnistream::ResourceIDPOD reID4("leo1", "Canada TOronto2");

    omnistream::AbstractIDPOD abs1(20000, 20000); // TODO
    omnistream::AbstractIDPOD abs2(20001, 10000);

    omnistream::IntermediateResultPartitionIDPOD Ipid1(abs1, 10); // TODO
    omnistream::IntermediateResultPartitionIDPOD Ipid2(abs2, 110);

    auto execution1 = omnistream::ExecutionAttemptIDPOD::randomId();
    auto execution2 = omnistream::ExecutionAttemptIDPOD::randomId();

    omnistream::ResultPartitionIDPOD rPod1(Ipid1, execution1);
    omnistream::ResultPartitionIDPOD rPod2(Ipid2, execution2);

    omnistream::ShuffleDescriptorPOD shuffleDescriptor(rPod1, false, true, reID); // TODO
    omnistream::ShuffleDescriptorPOD Slpd2(rPod1, false, true, reID2);
    omnistream::ShuffleDescriptorPOD Slpd3(rPod2, false, true, reID3);
    omnistream::ShuffleDescriptorPOD Slpd4(rPod2, false, true, reID4);

    omnistream::ResultPartitionDeploymentDescriptorPOD result_partition_deployment_descriptor_pod(
        shuffleDescriptor, 10, false, abs1, 13, Ipid1, 15, 16);

    omnistream::ResultPartitionDeploymentDescriptorPOD result_partition_deployment_descriptor_pod2(
        Slpd2, 10, false, abs1, 13, Ipid1, 15, 17);

    // todo
    std::vector<omnistream::ResultPartitionDeploymentDescriptorPOD> producedPartitions = {
        result_partition_deployment_descriptor_pod, result_partition_deployment_descriptor_pod2};

    omnistream::IntermediateDataSetIDPOD ibs1(20000, 20000);
    omnistream::IntermediateDataSetIDPOD ibs2(20001, 20000);
    omnistream::IntermediateDataSetIDPOD ibs3(20000, 20001);

    omnistream::ShuffleDescriptorPOD Slpd(rPod1, false, true, reID);

    std::vector<omnistream::ShuffleDescriptorPOD> shuffle = {Slpd, Slpd2};
    std::vector<omnistream::ShuffleDescriptorPOD> shuffle2 = {Slpd3, Slpd4};

    omnistream::InputGateDeploymentDescriptorPOD IDpod1(ibs1, 10, 10, shuffle);
    omnistream::InputGateDeploymentDescriptorPOD IDpod2(ibs2, 10, 10, shuffle2);

    // todo
    std::vector<omnistream::InputGateDeploymentDescriptorPOD> inputGates = {IDpod1, IDpod2};

    std::string taskStateSnapshot = "";
    omnistream::TaskDeploymentDescriptorPOD TDDpod(jobid1, producedPartitions, inputGates, taskStateSnapshot);
    omnistream::TaskDeploymentDescriptorPOD TDDpod2(jobid1, producedPartitions, inputGates, taskStateSnapshot);

    maps[TDDpod] = 1;
    EXPECT_EQ(maps[TDDpod], 1);

    maps[TDDpod] = 2;
    EXPECT_EQ(maps[TDDpod], 2);

    maps.erase(TDDpod);
    EXPECT_EQ(maps[TDDpod], 0);

    maps[TDDpod2] = 10;
    EXPECT_EQ(maps[TDDpod2], 10);
}

TEST(PODhascodeTest, PartitionDescriptorPOD)
{
    // AbstractIDPOD resultId;
    // int totalNumberOfPartitions;
    // IntermediateResultPartitionIDPOD partitionId;
    // int partitionType;
    // int numberOfSubpartitions;
    // int connectionIndex;

    std::unordered_map<omnistream::PartitionDescriptorPOD, int> maps;

    omnistream::AbstractIDPOD abs1(20000, 20000);
    omnistream::IntermediateResultPartitionIDPOD Irp(abs1, 15);

    omnistream::PartitionDescriptorPOD Part1(abs1, 10, Irp, 15, 16, 17);
    omnistream::PartitionDescriptorPOD Part2(abs1, 10, Irp, 15, 16, 17);

    maps[Part1] = 1;
    EXPECT_EQ(maps[Part1], 1);

    maps[Part1] = 2;
    EXPECT_EQ(maps[Part1], 2);

    maps.erase(Part1);
    EXPECT_EQ(maps[Part1], 0);

    maps[Part2] = 10;
    EXPECT_EQ(maps[Part2], 10);
}

TEST(PODhascodeTest, ResultSubpartitionInfoPOD)
{
    std::unordered_map<omnistream::ResultSubpartitionInfoPOD, int> maps;

    omnistream::ResultSubpartitionInfoPOD rIPOD(30, 40);
    omnistream::ResultSubpartitionInfoPOD rIPOD2(30, 40);

    maps[rIPOD] = 1;
    EXPECT_EQ(maps[rIPOD], 1);

    maps[rIPOD] = 2;
    EXPECT_EQ(maps[rIPOD], 2);

    maps.erase(rIPOD);
    EXPECT_EQ(maps[rIPOD], 0);
    maps[rIPOD2] = 10;
    EXPECT_EQ(maps[rIPOD], 10);
    EXPECT_EQ(maps[rIPOD2], 10);
}

//
//
//
//
// class PODhascodeTest {
//
//
//
// };

#endif // PODHASCODETEST_H
