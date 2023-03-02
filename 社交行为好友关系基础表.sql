


openid、好友openid、关系类型、游戏好友关系建立时间、高亲密度关系建立时间、亲密度等级、30天赠送总量、30天点赞总量、30天组队总次数、30天内是否互动频繁
字段含义：
1：微信好友,2：QQ好友,3：游戏好友,4：高亲密度关系（如基友、死党等）
我们用最亲密关系类型来表征. 如果3和1并存,只使用3. 如果1,3,4并存,只使用4
、游戏好友关系建立时间：、若不是游戏好友关系,用NA替代
、高亲密度关系建立时间：、若不是高亲密度关系,用NA替代
、亲密度等级、用已有的亲密度等级字段(0~15)


30天赠送总量：、30天中openid给好友openid所有赠送的物品的现金等价量
英雄/皮肤：LOGDT_FRIEND_PRESENTSEND ItemType ItemID
道具：LOGDT_FRIEND_PRESENTSEND ItemType ItemID
玫瑰：LOGDT_PROPINTIMACY_USE
只考虑消费点券的英雄.皮肤.道具好了, 计算为好友消费的点券总量、、、、、

、30天点赞总量：、30天中openid给好友openid的点赞总量、局后结算点赞LOGDT_LIKE_LOG
、30天组队总次数：、30天中openid和好友openid一起组队玩游戏的次数、可行性待定、组队定义：和该好友组队的次数、不包括3v3
、30天内是否互动频繁：、可以由上面三个指标综合来定（待定）、待定

游戏好友关系建立时间 只针对游戏好友(关系类型=3或者4), 如果只是sns关系(关系类型=1或2),该字段的值为NA

-------------------------------------------------------------------------------------------------------------------------------------------------
create table lzp_user_snsfriend_tmp as
select
    a1.vopenid,a1.izoneareaid,
    a2.vopenid as friendvopenid,a2.izoneareaid as friendizoneareaid,
    case when cast(substr(a2.izoneareaid,1,1) as bigint) in (1,2) then 1 --qq
         when cast(substr(a2.izoneareaid,1,1) as bigint) in (3,4) then 2 --wx
    end as friendtype,
    '1' as addfriendtime
from
    (select
        vopenid,izoneareaid,SnsFrienduid
    from
        (select --取一个月内最后一条记录
            a11.vopenid,a11.izoneareaid,a11.SnsFriendListStr
        from
            (select --sns登陆才有 
                vopenid,izoneareaid,SnsFriendListStr,dteventtime
            from ieg_tdbank::smoba_dsl_SnsFriendList_fht0
            where tdbank_imp_date >= '2021040900' and tdbank_imp_date <= '2021050923'
            and SnsFriendListStr is not null
            )a11
        join
            (select --一个月活跃玩家
                uid,vopenid,izoneareaid,dteventtime
            from hy_idog_oss::t_dw_smoba_acntstockextra
            where dtstatdate = '20210509'
            and regexp_replace(substr(dteventtime,1,10),'-','') >= '20210409'
            and regexp_replace(substr(dteventtime,1,10),'-','') <= '20210509'
            group by 
                uid,vopenid,izoneareaid,dteventtime
            )a12
            on a11.vopenid = a12.vopenid and a11.izoneareaid = a12.izoneareaid and a11.dteventtime = a12.dteventtime
        )lateral view explode(split(SnsFriendListStr,'\\,'))adtable as SnsFrienduid
    )a1
left join
    (select --仅有最近一个月记录 所以取就近一个日期
        uid,vopenid,izoneareaid
    from hy_idog_oss::smoba_qqweekreport_5v5battle_uid_openid_daily
    where dtstatdate = '20210801'
    group by uid,vopenid,izoneareaid
    )a2 on a1.SnsFrienduid = a2.uid

----------------------------------------------------------


create table lzp_user_tbfriend as
select
    a1.vopenid,a1.izoneareaid,
    a2.vopenid as friendvopenid,friendlogicworldid as friendizoneareaid,
    '3' as friendtype,
    addfrindtime as addfriendtime
from
    (select
        a12.vopenid,a12.izoneareaid,
        frienduid,friendlogicworldid,addfrindtime
    from
        (select --全量表 日150e
            uid,logicworldid,frienduid,friendlogicworldid,addfrindtime
        from ieg_tdbank::smoba_info_dsl_tbFriend_fdt0
        where tdbank_imp_date = '20210509' 
        )a11
    join
        (select --一个月活跃玩家
            uid,vopenid,izoneareaid
        from hy_idog_oss::t_dw_smoba_acntstockextra
        where dtstatdate = '20210509'
        and regexp_replace(substr(dteventtime,1,10),'-','') >= '20210409'
        and regexp_replace(substr(dteventtime,1,10),'-','') <= '20210509'
        group by 
            uid,vopenid,izoneareaid
        )a12
        on a11.uid = a12.uid and a11.logicworldid = a12.izoneareaid  --
    )a1
left join
    (select
        uid,vopenid
    from hy_idog_oss::smoba_qqweekreport_5v5battle_uid_openid_daily
    where dtstatdate = '20210801'
    group by 
        uid,vopenid
    )a2 on a1.frienduid = a2.uid

--------------------------------------------------------------------------------------
create table lzp_user_IntimacyRelation as
select
    a1.vopenid,a1.izoneareaid,a2.vopenid as friendvopenid,a1.friendizoneareaid,
    ftype,fvalue
from
    (select --登出表取亲密度关系
        vopenid,izoneareaid,
        split(IntimacyRelation,'\\+')[0] as frienduid,
        split(IntimacyRelation,'\\+')[1] as friendizoneareaid,
        split(IntimacyRelation,'\\+')[2] as ftype,
        split(IntimacyRelation,'\\+')[3] as fvalue
    from
        (select
            a11.vopenid,a11.izoneareaid,IntimacyRelationStr
        from
            (select 
                vopenid,izoneareaid,uid,IntimacyRelationStr,dteventtime
            from ieg_tdbank::smoba_dsl_AcntStockExtra_fht0
            where tdbank_imp_date >= '2021040900' and tdbank_imp_date <= '2021050923'
            and IntimacyRelationStr is not null
            )a11
        join
            (select --一个月活跃玩家
                uid,vopenid,izoneareaid,dteventtime
            from hy_idog_oss::t_dw_smoba_acntstockextra
            where dtstatdate = '20210509'
            and regexp_replace(substr(dteventtime,1,10),'-','') >= '20210409'
            and regexp_replace(substr(dteventtime,1,10),'-','') <= '20210509'
            group by 
                uid,vopenid,izoneareaid,dteventtime
            )a12
            on a11.vopenid = a12.vopenid and a11.izoneareaid = a12.izoneareaid and a11.dteventtime = a12.dteventtime
        )lateral view explode(split(IntimacyRelationStr,'\\,'))adtable as IntimacyRelation
    )a1
left join
    (select
        uid,vopenid
    from hy_idog_oss::smoba_qqweekreport_5v5battle_uid_openid_daily
    where dtstatdate = '20210801'
    group by uid,vopenid --
    )a2 on a1.frienduid = a2.uid;






create table lzp_user_Relation_time as
select
    a1.vopenid,a1.izoneareaid,
    a2.vopenid as friendvopenid,IntimacyLogicWordId as friendizoneareaid,
    highrela_time
from
    (select 
        vopenid,izoneareaid,IntimacyUid,IntimacyLogicWordId,max(dteventtime) as highrela_time
    from ieg_tdbank::smoba_dsl_chgintimacyrelation_fht0
    where tdbank_imp_date <= '2021050900'
    and ChgIntimacyType = 1
    group by vopenid,izoneareaid,IntimacyUid,IntimacyLogicWordId
    )a1
left join
    (select
        uid,vopenid
    from hy_idog_oss::smoba_qqweekreport_5v5battle_uid_openid_daily
    where dtstatdate = '20210801'
    group by 
        uid,vopenid
    )a2 on a1.IntimacyUid = a2.uid


----------------------------------------------------------------------------------------------------------
create table lzp_user_friend_basic3 as
    select
        b1.vopenid,b1.izoneareaid,b1.friendvopenid,b1.friendizoneareaid,
        friendtype,addfriendtime,fvalue
        ,likecnt,teambatcnt,CouponsCost
    from
        (select
            vopenid,izoneareaid,friendvopenid,friendizoneareaid,
            max(friendtype) as friendtype,
            max(addfriendtime) as addfriendtime,
            max(fvalue) as fvalue
        from
            (select *,'1' AS fvalue from lzp_user_snsfriend_tmp
            where vopenid is not null and izoneareaid is not null and friendvopenid is not null and friendizoneareaid is not null
            
            union all

            select *,'1' AS fvalue from lzp_user_tbfriend
            where vopenid is not null and izoneareaid is not null and friendvopenid is not null and friendizoneareaid is not null
            and addfriendtime is not null

            union all

            SELECT vopenid,izoneareaid,friendvopenid,friendizoneareaid,
                   '4' as friendtype,'1' as addfriendtime,fvalue
            FROM lzp_user_IntimacyRelation
            where vopenid is not null and izoneareaid is not null and friendvopenid is not null and friendizoneareaid is not null
            and ftype is not null
            )
        group by vopenid,izoneareaid,friendvopenid,friendizoneareaid
        )b1

    left join

        (select * from lzp_user_presentsend
        )b2 on b1.vopenid = b2.vopenid
        and b1.izoneareaid = b2.izoneareaid
        and b1.friendvopenid = b2.friendvopenid
        and b1.friendizoneareaid = b2.friendlogicworldid

    left join

        (select * from lzp_user_likecnt
        )b3 on b1.vopenid = b3.vopenid
        and b1.izoneareaid = b3.izoneareaid
        and b1.friendvopenid = b3.friendvopenid
        and b1.friendizoneareaid = b3.likelogicwordid

    left join

        (select * from lzp_user_teambatcnt
        )b4 on b1.vopenid = b4.vopenid
        and b1.izoneareaid = b4.izoneareaid
        and b1.friendvopenid = b4.friendvopenid
        and b1.friendizoneareaid = b4.izoneareaid0


-------------------------------------------------------------------------------------------


create table lzp_user_friendrelation_basic as
select
    a1.vopenid,a1.izoneareaid,a1.friendvopenid,a1.friendizoneareaid,
    friendtype,addfriendtime,highrela_time,flevel,
    couponscost,likecnt,teambatcnt
from
    (select 
        vopenid,izoneareaid,friendvopenid,friendizoneareaid,
        case when friendtype = 1 then 2 
             when friendtype = 2 then 1
        else friendtype end as friendtype,
        case when addfriendtime = 1 then 'NA' else addfriendtime end as addfriendtime,
        floor(fvalue/100) as flevel,
        likecnt,teambatcnt,couponscost
    from ieg_sgame_internal::lzp_user_friend_basic3
    )a1
left join
    (select
        *
    from lzp_user_Relation_time
    where vopenid is not null and izoneareaid is not null and friendvopenid is not null and friendizoneareaid is not null
    and highrela_time is not null
    )a2 on  a1.vopenid = a2.vopenid
        and a1.izoneareaid = a2.izoneareaid
        and a1.friendvopenid = a2.friendvopenid
        and a1.friendizoneareaid = a2.friendizoneareaid



show rowcount lzp_user_friendrelation_basic

select * from lzp_user_friendrelation_basic where vopenid = 'owanlsi1ZQ7ERNFUdB-zouV7nPsE'