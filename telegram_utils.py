async def update_group_logos_async():
    """
    Iterates through all monitored groups in the database and updates their profile photos.
    """
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        logger.warning("LogoUpdater: Telegram client not connected. Skipping.")
        return

    db_session = get_session()
    try:
        monitored_groups = db_session.query(MonitoredGroup).all()
        if not monitored_groups:
            logger.info("LogoUpdater: No monitored groups to update.")
            return

        logger.info(f"LogoUpdater: Starting check for {len(monitored_groups)} groups.")
        updated_count = 0

        for group in monitored_groups:
            try:
                # Use the numeric ID if possible, otherwise the username string
                identifier = int(group.group_identifier)
            except ValueError:
                identifier = group.group_identifier

            try:
                entity = await client.get_entity(identifier)
                
                logo_dir = os.path.join(basedir, 'static', 'logos')
                os.makedirs(logo_dir, exist_ok=True)
                logo_filename = f"{entity.id}.jpg"
                logo_abs_path = os.path.join(logo_dir, logo_filename)
                
                # download_profile_photo returns None if there's no new photo
                path = await client.download_profile_photo(entity, file=logo_abs_path)
                
                if path:
                    logo_rel_path = f"logos/{logo_filename}"
                    if group.logo_path != logo_rel_path:
                        group.logo_path = logo_rel_path
                        db_session.commit()
                        logger.info(f"LogoUpdater: Updated logo for group '{group.group_name}'.")
                        updated_count += 1
                
                # Also update group name if it has changed
                if entity.title != group.group_name:
                    logger.info(f"LogoUpdater: Group name for '{group.group_name}' changed to '{entity.title}'. Updating.")
                    group.group_name = entity.title
                    db_session.commit()


            except Exception as e:
                logger.error(f"LogoUpdater: Could not update logo for group '{group.group_name}' (ID: {group.group_identifier}). Reason: {e}")
                continue
        
        if updated_count > 0:
            logger.info(f"LogoUpdater: Finished. Updated logos for {updated_count} groups.")
        else:
            logger.info("LogoUpdater: Finished. No new logos found.")

    finally:
        db_session.close()